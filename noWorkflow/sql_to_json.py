import sqlite3
import json
import ast
import pandas
import os
from os.path import expanduser
import subprocess
import sys
from io import StringIO
import hashlib
BUF_SIZE = 65536

def get_info_from_sql(input_db_file, run_num):
    """ queries noWorkflow sql database """

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()

    # process nodes
    c.execute('SELECT trial_id, id, name, return_value, line from function_activation where trial_id = ?', (run_num,))
    script_steps = c.fetchall()

    # file io nodes
    c.execute('SELECT trial_id, name, function_activation_id, mode, content_hash_after from file_access where trial_id = ?' , (run_num, ))
    files = c.fetchall()

    # dict for easier access to file info
    temp = {}
    for f in files:
        d = {"name": f[1], "mode": f[3], "hash" : f[4]}
        temp[f[2]] = d
    files = temp

    # functions
    c.execute('SELECT name, trial_id, last_line from function_def where trial_id = ?', (run_num, ))
    func_ends = c.fetchall()

    # dict for easier access to func_ends. used for collapsing nodes
    temp = {}
    end_funcs = {}
    for f in func_ends:
        temp[f[0]] = f[2]
        end_funcs[f[2]] = f[0]
    func_ends = temp

    # if f has return value, f[2]-=1
    # so last line detected correctly
    # last line informs the finish node + allows for sequential functions
    for f in func_ends:
        c.execute('SELECT trial_id, name, return_value from function_activation where trial_id = ? and name = ?', (run_num, f, ))
        calls = c.fetchall()
        for call in calls:
            if call[2]!="None":
                func_ends[f]-=1
                temp = func_ends[f]
                end_funcs[temp]=f

    c.close()

    return script_steps, files, func_ends, end_funcs

def get_script_file(input_db_file, trial_num):
    """ query the db for the code hash for scripts in workflows
    use this script for building the script_line_dict
    so that workflow process labels are correct """

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()

    # process nodes
    c.execute('SELECT id, code_hash from trial where id = ?', (trial_num,))
    code_hash = c.fetchone()[1]

    dir_code = code_hash[0:2]
    file_name = code_hash[2:]

    db_dir = "/".join(input_db_file.split("/")[0:-1])
    path_to_file = os.path.join(db_dir, "content", dir_code, file_name)

    return path_to_file

def get_script_line_dict(path_to_file):
    """ keys = line_number, values = line
    used for labelling the process nodes """

    script_line_dict = {}

    with open(path_to_file) as f:
        for i, line in enumerate(f):
            script_line_dict[i+1]=line.strip()

    return script_line_dict

def get_defaults(script_name):
    """ sets default required fields for the Prov-JSON file, ie environment node
    variable 'rdt:script' is the script name or the first script in the workflow.
    """
    result, activity_d, environment_d = {}, {}, {}

    environment_d['rdt:language'] = "R"
    environment_d["rdt:script"] = script_name
    activity_d['environment'] = environment_d

    result['activity']= activity_d

    keys = ["entity", "wasInformedBy", "wasGeneratedBy", "used"]
    for i in range (0, len(keys)):
        result[keys[i]]={}

    return result

def add_informs_edge(result, prev_p, current_p, e_count):
    """ adds informs edge between steps in the script or between script nodes
    to ensure correct sequential layout of process nodes """

    current_informs_edge = {}
    current_informs_edge['prov:informant'] = prev_p
    current_informs_edge['prov:informed'] = current_p

    ekey_string = "e" + str(e_count)
    e_count+=1

    result['wasInformedBy'][ekey_string] = current_informs_edge

    return e_count

def add_start_node(result, step, p_count, current_line = None):
    """ adds start node and edge for current step """

    # make node
    start_node_d = {}
    start_node_d['rdt:type'] = "Start"
    start_node_d["rdt:elapsedTime"] = "0.5"
    keys = ["rdt:scriptNum", "rdt:startLine", "rdt:startCol", "rdt:endLine", "rdt:endCol"]
    for key in keys:
        start_node_d[key] = "NA"

    # choose most descriptive label for the node. Usually the line in the script, not the noWorkflow default
    if current_line:
        start_node_d['rdt:name'] = current_line
    else:
        start_node_d['rdt:name'] = step[2]

    pkey_string = "p" + str(p_count)
    p_count+=1
    prev_p = pkey_string

    # add node
    result['activity'][pkey_string] = start_node_d

    return prev_p, p_count

def add_end_node(result, p_count, name):
    """ makes Finish node """

    # make node
    end_node_d = {}
    end_node_d['rdt:name'] = name
    end_node_d['rdt:type'] = "Finish"
    end_node_d["rdt:elapsedTime"] = "0.5"
    keys = ["rdt:scriptNum", "rdt:startLine", "rdt:startCol", "rdt:endLine", "rdt:endCol"]
    for key in keys:
        end_node_d[key] = "NA"

    # add node
    pkey_string = "p" + str(p_count)
    p_count += 1
    result['activity'][pkey_string] = end_node_d

    return pkey_string, p_count

def add_process(result, p_count, s, script_name, current_line):
    """ adds process node and edge for each step in script_steps """

    # defaults for all process nodes
    current_process_node = {}
    current_process_node['rdt:type'] = "Operation"
    current_process_node["rdt:elapsedTime"] = "0.5"
    current_process_node["rdt:startLine"], current_process_node["rdt:endLine"] = str(s[4]), str(s[4])

    # use the corresponding line in script as the label
    line_label = current_line.strip()

    current_process_node['rdt:name'] = line_label
    current_process_node["rdt:startCol"] = str(0)
    current_process_node["rdt:endCol"] = str(len(line_label))
    current_process_node["rdt:scriptNum"] = str(0)

    # add the node
    pkey_string = "p" + str(p_count)
    p_count += 1
    result["activity"][pkey_string] = current_process_node

    return p_count, pkey_string

def check_input_files_and_add(input_db_file, run_num, files, function_activation_id):
    """ if input file not detected bc of lacking "with open() as f" format
    detect input file by using db info
    makes sure that the string is a file/path
    if file/path: add to files dict
    then use add_file in make_dict """

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()
    c.execute('SELECT trial_id, value, function_activation_id, name from object_value where trial_id = ? and function_activation_id = ? and name = ?', (run_num, function_activation_id, "filepath_or_buffer"))
    temp = c.fetchone()

    # verify that it is a filename
    try:
        filename = temp[1].strip("'")
    except:
        return

    #generate a sha1 hash
    sha1 = hashlib.sha1()
    with open(filename, 'rb') as f:
        while True:
            data = f.read(BUF_SIZE)
            if not data:
                break
            sha1.update(data)
    h = format(sha1.hexdigest())

    # make new entry with this hash
    temp_dict = {'hash': h, 'name': filename, 'mode': 'r'}
    files[temp[2]]= temp_dict

def check_input_intermediate_value_and_add(input_db_file, run_num, d_count, e_count, result, function_activation_id, prev_p, script_name, data_dir, int_values):
    """ if to_csv does not have incoming data node bc of lacking recent assignment
    checks if intermediate value already exists in db
    if not, adds node and edge """

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()
    c.execute('SELECT trial_id, value, function_activation_id, name from object_value where trial_id = ? and function_activation_id = ? and name = ?', (run_num, function_activation_id, "self", ))
    temp = c.fetchone()

    try:
        if temp[1] in int_values:
            # if incoming value already exists, do not add anything
            return d_count, e_count, None, None
        else:
            # add data node by reformatting s and calling add_data()
            lst = list(s)
            lst[3]=temp[1]
            s = tuple(lst)
            d_count, e_count, dkey_string = add_data(result, s, d_count, e_count, prev_p, script_name, data_dir, to_csv = True)
            return d_count, e_count, dkey_string, s[3]
    except:
        return d_count, e_count, None, None

def add_file_node(script, current_link_dict, d_count, result, data_dict, activation_id_to_p_string, function_activation_id, h, first_step, outfiles):
    """ adds a file node, called by add_file """

    #make file node
    current_file_node = {}
    current_file_node['rdt:name'] = script
    current_file_node['rdt:type'] = "File"
    keys = ['rdt:scope', "rdt:fromEnv", "rdt:timestamp", "rdt:location"]
    values = ["undefined", "FALSE", "", ""]
    for i in range (0, len(keys)):
        current_file_node[keys[i]] = values[i]

    split_file_path = current_link_dict['name'].strip('.')
    split = split_file_path.split("/")[1:]

    if len(split) >0:
        # if relative path provided
        if split[0] == "results" or split[0] == "data":
            temp = "/".join(split)
        # if full path provided
        else:
            if "data" in split:
                start = split.index("data")
                temp = "/".join(split[start:])
            elif "results" in split:
                start = split.index("results")
                temp = "/".join(split[start:])

        # set value/relative path according to file's parent directory.
        current_file_node['rdt:value'] = "../" + temp
    else:
        # if only file name, no path provided
        # reading/writing files to same script dir, no additional .. or / needed
        current_file_node['rdt:value'] = split_file_path

    # add file node
    dkey_string = "d" + str(d_count)
    d_count+=1
    result["entity"][dkey_string] = current_file_node

    # add to dict of edges to make connections b/w graphs
    data_dict[script] = dkey_string

    if "w" in current_link_dict['mode']:
    # if file created, add to outfiles dict for linking graphs
        inner_dict = {'data_node_num': dkey_string, 'hash_out': h}
        inner_dict['source']= activation_id_to_p_string[int(function_activation_id)]

        if first_step[2] in outfiles.keys():
            temp = outfiles[first_step[2]]
            temp[script] = inner_dict
            outfiles[first_step[2]] = temp
        else:
            middle_dict = {script : inner_dict}
            outfiles[first_step[2]] = middle_dict

    return d_count, dkey_string

def add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles):
    """ adds a file edge, called by add_file """

    # make edge
    current_edge_node = {}
    current_edge_node['prov:activity'] = current_p
    current_edge_node['prov:entity'] = dkey_string

    # add edge
    e_string = "e" + str(e_count)
    e_count+=1

    # choose the correct category based on mode
    # r --> used, w --> wasGeneratedBy
    if "r" in current_link_dict['mode']:
        result['used'][e_string] = current_edge_node
    else:
        result['wasGeneratedBy'][e_string] = current_edge_node

    return e_count

def add_file(result, files, d_count, e_count, current_p, s, outfiles, first_step, activation_id_to_p_string, data_dict):
    """ uses files dict to add file nodes and access edges to the dictionary
    uses outfiles dict to check if file already exists from a previous script """

    dkey_string = -1

    # get file_name
    current_link_dict = files[s[1]]
    path_array = current_link_dict['name'].split("/")

    #get hash
    file_entry = files[s[1]]
    h = file_entry['hash']

    for script in outfiles.keys():
        for outfile in outfiles[script]:
            # if already seen
            if outfile == path_array[-1]:
                if outfiles[script][outfile]['hash_out']==None or h==None or outfiles[script][outfile]['hash_out']==h:
                    # do not add new node, but return d_key_string
                    dkey_string = data_dict[path_array[-1]]
                # else: no match, add new node.

    if dkey_string == -1: # if not seen yet, add node
        d_count, dkey_string = add_file_node(path_array[-1], current_link_dict, d_count, result, data_dict, activation_id_to_p_string, s[1], h, first_step, outfiles)
        # the addition is overwriting instead of adding to the dict

    # no matter if new or seen, add new dependent edge
    e_count = add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles)

    return d_count, e_count

def get_data_frame(s):
    """ tries to convert to data to df to save intermediate value and improve vis """

    df = None
    if s[3]!=None:

        y = s[3].split("\n")
        first_line = y[0].strip()
        last_line = y[-1].strip()

        if "Name" in last_line:
            # get col names from last line
            temp = s[3].split("Name")[0]
            df_as_string = StringIO(temp)
            col = last_line.split()[1].strip(",")
            df = pandas.read_csv(df_as_string, delim_whitespace = True, index_col = 0, names = [col])

        elif "\\" in first_line:

            # TO DO

            df_as_string = StringIO(s[3])
            df = pandas.read_csv(df_as_string, delim_whitespace = True, index_col = 0)

        elif "Unnamed" in first_line:
            # get col names from first line
            # remove extra unnamed col with unnecessary index numbering

            for i in range (1, len(y)):
                y[i]=y[i][1:]
            y = "\n".join(y[1:])
            col = first_line.split()[2:]
            df_as_string = StringIO(y)
            df = pandas.read_csv(df_as_string, delim_whitespace = True, names = col, index_col = 0)

        else:

            # usual case
            df_as_string = StringIO(s[3])
            df = pandas.read_csv(df_as_string, index_col = 0)

            # if all data goes into 1 list or the other
            if len(df.columns) >0 or len(df.index)>0:
                # get the col names from the first line
                col = y[0].split()
                y = "\n".join(y[1:])
                df_as_string = StringIO(y)
                df = pandas.read_csv(df_as_string, delim_whitespace = True, names = col)

        # if some sort of object, module, single integer, or non-df
        if df.empty:
            return s[3]

    return df

def add_data_node(result, d_count, current_data_node):
    """  called by add_data """

    # add data node
    dkey_string = "d" + str(d_count)
    d_count+=1
    result["entity"][dkey_string] = current_data_node

    return dkey_string, d_count

def add_data_edge(e_count, current_p, current_data_node, result, dkey_string):
    """  called by add_data """

    # make edge
    current_edge_node = {}
    current_edge_node['prov:activity'] = current_p
    current_edge_node['prov:entity'] = dkey_string

    # add edge
    e_string = "e" + str(e_count)
    e_count+=1
    result['wasGeneratedBy'][e_string] = current_edge_node

    return e_count, dkey_string

def add_data(result, s, d_count, e_count, current_p, script_name, data_dir, to_csv = False):
    """ makes intermediate data node if process had return value
    if dataframe, make a snapshot csv
    else, make a normal data node as a string """

    # make data node
    current_data_node = {}
    current_data_node['rdt:name'] = "data"
    current_data_node['rdt:scope'] = "R_GlobalEnv"
    keys = ["rdt:fromEnv", "rdt:timestamp", "rdt:location"]
    values = ["FALSE", "", ""]
    for i in range (0, len(keys)):
        current_data_node[keys[i]] = values[i]

    # If possible, convert data to dataframe to save intermediate values and improve vis
    df = get_data_frame(s)

    if isinstance(df, pandas.core.frame.DataFrame):
        current_data_node['rdt:type'] = "Snapshot"
        # make dir if it doesn't exist
        filename = "line" + str(s[4]) + "data.csv"
        script = script_name.split("/")[-1].strip(".py")
        directory = data_dir + "/intermediate_values_of_" + script + "_data/"
        if not os.path.exists(directory):
            os.makedirs(directory)
        path = directory + filename

        # write to csv
        df.to_csv(path)
        current_data_node['rdt:value'] = "../data/intermediate_values_of_" + script + "_data/" + filename

        if not to_csv: # add node and edge
            dkey_string, d_count = add_data_node(result, d_count, current_data_node)
            e_count, dkey_string = add_data_edge(e_count, current_p, current_data_node, result, dkey_string)
        else: # if csv, only want to add node, not incoming edge
            dkey_string, d_count = add_data_node(result, d_count, current_data_node)

        return d_count, e_count, dkey_string

    elif isinstance(df, str):
        # if the return value is a non df, like a string, single integer, or module
        current_data_node['rdt:type'] = "Data"
        current_data_node['rdt:value'] = s[3].strip("'").strip()
        dkey_string, d_count = add_data_node(result, d_count, current_data_node)
        e_count, dkey_string = add_data_edge(e_count, current_p, current_data_node, result, dkey_string)
        return d_count, e_count, dkey_string

    else:
        # if the return value is None, return same d_count and e_count
        return d_count, e_count, None

def get_arguments_from_sql(input_db_file, return_value, run_num, activation_id_to_p_string, lowest_process_num):
    """ queries sql db to find process nodes dependent on intermediate return values
    returns the process_string of these processes """

    target_processes = []
    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()

    c.execute('SELECT trial_id, value, function_activation_id from object_value where trial_id = ? and value = ?', (run_num, return_value, ))
    all_dep_processes = c.fetchall()

    # get all dependent processes and convert to p_string
    for p in all_dep_processes:
        process = p[2]
        # prevent upward-pointing edges
        if process >= lowest_process_num:
            p_string = activation_id_to_p_string[process]
            target_processes.append(p_string)

    return target_processes

def int_data_to_process(dkey_string, process_string, e_count, result):
    """ adds edge from intermediate data node to dependent process node
    uses int_values, which is made by get_arguments_from_sql() """

    # make edge
    current_edge_node = {}
    current_edge_node['prov:activity'] = process_string
    current_edge_node['prov:entity'] = dkey_string

    # add edge
    e_string = "e" + str(e_count)
    e_count+=1
    result['used'][e_string] = current_edge_node

    return e_count

def make_dict(script_steps, files, input_db_file, run_num, func_ends, end_funcs, p_count, d_count, e_count, outfiles, result, data_dict, finish_node, script_name, loop_dict, data_dir, script_line_dict):
    """ uses the information from the database
    to make a dictionary compatible with Prov-JSON format
    """

    # if first script in list, set up the default formats
    if len(result.keys()) == 0:
        result = get_defaults(script_name)

    # if not first script, add informs edge between
    # the Finish of the previous script and the Start of the current script
    if finish_node!= None:
        current_p = "p" + str(p_count)
        e_count = add_informs_edge(result, finish_node, current_p, e_count)

    # initialize per-script variables
    process_stack, loop_name_stack, loop_stack, function_stack = [], [], [], []
    int_values, int_dkey_strings, lowest_process_num = [], [], []
    dkey_string = -1
    activation_id_to_p_string = {}

    prev_p, p_count = add_start_node(result, script_steps[0], p_count)
    process_stack.append(script_steps[0][4])
    function_stack.append(script_steps[0][4])

    prev_process_label = ""
    current_line = script_line_dict[script_steps[1][4]]

    # iterate through each line in the script
    for i in range (1, len(script_steps)):
        s = script_steps[i]

        # get the next line of the script to check for repeated process labels
        try:
            next_s = script_steps[i+1]
            next_process_label = script_line_dict[next_s[4]]
        except: # last line index will be out of range
            next_process_label = ""

        if "print" not in current_line:

            # if loop has ended on current step, add finish node
            if len(loop_stack)>0 and s[4] >= loop_stack[-1]:
                # get the function name
                func_name = loop_name_stack.pop()

                # add the finish node and pop from the stacks
                current_p, p_count = add_end_node(result, p_count, func_name)
                process_stack.pop()
                loop_stack.pop()

                # add informs edge between last process node in loop and the finish node of the loop
                e_count = add_informs_edge(result, prev_p, current_p, e_count)
                prev_p = "p" + str(p_count-1)

            # if current step is a function, add start node
            # store the function_activation_id in stack to make Finish node later
            if s[2] in func_ends:
                current_p, p_count = add_start_node(result, s, p_count)
                process_stack.append(func_ends[s[2]])
                function_stack.append(func_ends[s[2]])

            # if current_step is the start of a loop, add start node
            # store the last line in loop in stack to make Finish node later
            elif s[4] in loop_dict.keys():
                current_p, p_count = add_start_node(result, s, p_count, current_line.strip())
                process_stack.append(loop_dict[s[4]])
                loop_name_stack.append(current_line.strip())
                loop_stack.append(loop_dict[s[4]])

            else:
                # if new line (to prevent identical process nodes)
                if current_line != prev_process_label:
                    p_count, current_p = add_process(result, p_count, s, script_name, current_line)

                    # special case checks
                    if "pandas.read_csv" in current_line and "with open" not in prev_process_label:
                        # if reading csv, ensure that input file is detected, even w/o "with open() as f" syntax
                        check_input_files_and_add(input_db_file, run_num, files, s[1])

                    if "to_csv" in current_line:
                        # get intermediate value before writing to file
                        d_count, e_count, dkey_string, int_value = check_input_intermediate_value_and_add(input_db_file, run_num, d_count, e_count, result, s[1], prev_p, script_name, data_dir, int_values)
                        # if return value and dkey_string exists, add info to int_values so an edge can be created
                        if dkey_string != None:
                            int_values.append(int_value)
                            lowest_process_num.append(s[1])
                            int_dkey_strings.append(dkey_string)

            # dict for use in get_arguments_from_sql
            activation_id_to_p_string[s[1]] = current_p

            # if process node reads or writes to file, add file nodes and edges
            if s[1] in files.keys():
                d_count, e_count = add_file(result, files, d_count, e_count, current_p, s, outfiles, script_steps[0], activation_id_to_p_string, data_dict)
                if files[s[1]]['mode']=="r":
                    # if a read file, add another entry to the files dict for writing to hashtable
                    files[s[1]]['nodenum']=d_count-1

            # if process is not redundant
            if current_line != next_process_label:
                # if process node has return statement, make intermediate data node and edges
                if s[3] != "None":
                    d_count, e_count, dkey_string = add_data(result, s, d_count, e_count, current_p, script_name, data_dir)

                    # if return value and dkey_string exists, add info to data structures
                    if dkey_string != None:
                        int_values.append(s[3])
                        lowest_process_num.append(s[1])
                        int_dkey_strings.append(dkey_string)

            if current_line != prev_process_label:
                # add_informs_edge between all process nodes
                e_count = add_informs_edge(result, prev_p, current_p, e_count)
                prev_p = "p" + str(p_count-1)

            # if function, NOT LOOP, has ended on current step, add finish node
            if s[4] == function_stack[-1]:
                # get the function name
                func_name = end_funcs[s[4]]

                # add the finish node and pop from stacks
                current_p, p_count = add_end_node(result, p_count, func_name)
                process_stack.pop()
                function_stack.pop()

                # add informs edge between last process node in loop and the finish node of the loop
                e_count = add_informs_edge(result, prev_p, current_p, e_count)
                prev_p = "p" + str(p_count-1)

            prev_process_label = current_line
            current_line = next_process_label

    # after all steps in script done
    # add finish nodes (both loops and functions)
    # and informs edges for the rest of the process_stack
    while len(process_stack)>1:
        func_line = process_stack.pop()
        try: # get the func name
            func_name = end_funcs[func_line]
        except: # get the loop name
            func_name = loop_name_stack.pop()

        # add the finish node and edge
        current_p, p_count = add_end_node(result, p_count, func_name)

        e_count = add_informs_edge(result, prev_p, current_p, e_count)
        prev_p = "p" + str(p_count-1)

    # add finish node and final informs edge for the script
    current_p, p_count = add_end_node(result, p_count, script_steps[0][2])
    e_count = add_informs_edge(result, prev_p, current_p, e_count)

    # adds used edges using dependencies from database table: object_value
    for i in range (0, len(int_values)):
        return_value = int_values[i]
        target_processes = get_arguments_from_sql(input_db_file, return_value, run_num, activation_id_to_p_string, lowest_process_num[i])
        for process in target_processes:
            e_count = int_data_to_process(int_dkey_strings[i], process, e_count, result)

    return result, p_count, d_count, e_count, outfiles, current_p, files

def get_loop_locations(script_name):
    """ uses ast module to find the start and end lines of for and while loops
    to allow for collapsible nodes for loops (as well as functions) """

    loop_dict = {}

    with open(script_name) as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, (ast.For, ast.While)):
            # keys = start line, values = finish line
            # offset by 1 to match with script_steps numbering
            loop_dict[node.lineno] = node.body[-1].lineno+1

    return loop_dict

def write_json(dictionary, output_json_file):
    with open(output_json_file, 'w') as outfile:
        json.dump(dictionary, outfile, default=lambda temp: json.loads(temp.to_json()))

def add_default_hash_node(current_entry, isRead, script_name, ddg_path, end_of_file_path, new_entries):
    """ adds default values to the hash_node
    adds hash_node to the new_entries"""

    if isRead:
        current_entry['ReadWrite'] = "read"
    else:
        current_entry['ReadWrite'] = "write"

    current_entry['ScriptPath'] = script_name
    current_entry['DDGPath'] = ddg_path

    # # use overlap of paths to get the full, non relative file path
    i=0
    start_of_file_path = ""
    while script_name[i] == ddg_path[i]:
        start_of_file_path+=script_name[i]
        i+=1
    file_path = start_of_file_path + end_of_file_path
    current_entry['FilePath'] = file_path

    keys = ["NodePath", "Timestamp", "Value"]
    for key in keys:
        current_entry[key] = ""
    new_entries.append(current_entry)

def write_to_hashtable(files, outfiles, script_name, ddg_path):
    """ records file IO using files and outfiles into hash_nodes
    that are added to new_entries
    which is written/added to home/.ddg/hashtable.json """

    new_entries = []

    # first, convert files
    # from format function_activation_id-->entry
    # to format filename -->rest of the info in entry
    temp_out_files = {}
    for f in files:
        if files[f]['mode'] == "w":
            temp_out_files[files[f]['name'].split("/")[-1]] = {'full_path': files[f]['name']}
        elif files[f]['mode'] == "r":
            # in files using files
            current_entry = {}
            current_entry['SHA1Hash'] = files[f]["hash"]
            current_entry['NodeNumber'] = str(files[f]["nodenum"])
            add_default_hash_node(current_entry, True, script_name, ddg_path, files[f]['name'].strip("../"), new_entries)

    # outfiles using outfiles and temp_out_files
    for script in outfiles.keys():
        for output_key in outfiles[script]:
            # use outfiles and temp_out_files to set entries to dictionary
            current_entry = {}
            current_entry['SHA1Hash'] = outfiles[script][output_key]["hash_out"]
            current_entry['NodeNumber'] = outfiles[script][output_key]["data_node_num"].strip("d")
            add_default_hash_node(current_entry, False, script_name, ddg_path, temp_out_files[output_key]['full_path'].strip("../"), new_entries)

    # write new_entries to the home dir
    # temp_file_path = "/Users/jen/Desktop/temp.json"
    home = expanduser("~")
    hashtable_path = os.path.join(home, ".ddg", "hashtable.json")

    if not os.path.isfile(hashtable_path):
        with open(hashtable_path, 'w') as f:
            json.dump(new_entries, f)
    else:
        with open(hashtable_path, 'r') as f:
            try:
                existing_entries = json.load(f)
            except: # if the file is empty at first, need to initialize existing_entries
                existing_entries = []

            for entry in new_entries:
                if entry not in existing_entries:
                    # if entry does not already exist
                    existing_entries.append(entry)

        with open(hashtable_path, 'w') as f:
            json.dump(existing_entries, f)

def link_DDGs(trial_num_list, input_db_file, output_json_file, data_dir, script_name):
    """ input: db_file generated by noworkflow
    target path where the Prov-JSON file will be written
    and a list of trial numbers that will be linked together into a DDG
    where trial numbers correspond to individual scripts stored in the noworkflow database
    If only 1 trial_num is provided, a single script is analyzed

    output: prov-json file that can be opened in DDG Explorer

    used by get_prov(script_path) """

    # initialize variables that will carry over from 1 script to the next
    p_count, d_count, e_count = 1, 1, 1
    result, outfiles, data_dict = {}, {}, {}
    finish_node = None

    ddg_path = "/".join(output_json_file.split("/")[:-1])

    # for each trial, query and add to the result
    for trial_num in trial_num_list:
        script_steps, files, func_ends, end_funcs = get_info_from_sql(input_db_file, trial_num)
        path_to_file = get_script_file(input_db_file, trial_num) # location in content db
        script_line_dict = get_script_line_dict(path_to_file)
        loop_dict = get_loop_locations(path_to_file)
        result, p_count, d_count, e_count, outfiles, finish_node, files = make_dict(script_steps, files, input_db_file, trial_num, func_ends, end_funcs, p_count, d_count, e_count, outfiles, result, data_dict, finish_node, script_name, loop_dict, data_dir, script_line_dict)

        # TO DO
        write_to_hashtable(files, outfiles, script_name, ddg_path)

    # Write to file
    write_json(result, output_json_file)

def get_paths(script_path):
    """ uses os and sys to get the paths to call link_DDGs
    used when the user inports sql_to_json in their script.py
    and then runs python script.py or now run script.py while in the scripts dir
    produces another directory, python_prov, in the project_dir,
    where prov-JSON files are stored

    input script_path is __file__, where this is called from the script.py

    used by get_prov(script_path)"""
    scripts_dir = "/".join(script_path.split("/")[:-1])
    project_dir = "/".join(scripts_dir.split("/")[:-1])
    data_dir = os.path.join(project_dir, "data")
    prov_dir = os.path.join(project_dir, "python_prov")

    if not os.path.exists(prov_dir):
        os.makedirs(prov_dir)

    script_name = script_path.split("/")[-1].split(".")[0]
    json_name = script_name + ".json"

    output_json_file = os.path.join(prov_dir, json_name)

    return output_json_file, data_dir

def get_prov(input_db_file, script_path, trial_num_list):
    output_json_file, data_dir = get_paths(script_path)
    link_DDGs(trial_num_list, input_db_file, output_json_file, data_dir, script_path)

def main():

    trial_num_list = []
    for i in range (1, len(sys.argv)):
        trial_num_list.append(int(sys.argv[i]))

    if len(trial_num_list) ==0:
        print("usage: sql_to_json <trial_num_1> [trial_num_2] [trial_num_3]...")

    else:
        # call this file while in the scripts dir, after calling now run
        scripts_dir = os.getcwd()
        noworkflow_db = ".noworkflow/db.sqlite"
        input_db_file = os.path.join(scripts_dir, noworkflow_db)

        # script_name
        db = sqlite3.connect(input_db_file, uri=True)
        c = db.cursor()
        c.execute('SELECT id, script from trial where id = ? ', (trial_num_list[0], ))
        temp = c.fetchone()
        script_name = temp[1]

        script_path = os.path.join(scripts_dir, script_name)

        # single script results -->python prov dir with name script.json
        if len(trial_num_list)==1:
            get_prov(input_db_file, script_path, trial_num_list)
            print("Wrote script prov for " + script_name + " to " + script_path)

        # workflow results -->results dir with name script1.json
        else:
            output_json_file, data_dir = get_paths(script_path)
            c.execute('SELECT id, script from trial where id = ? ', (trial_num_list[-1], ))
            temp = c.fetchone()
            last_script_name = temp[1]
            output_json_file = "/Users/jen/Desktop/newNow/results/workflow_" + script_name+ "_to_" + last_script_name +".json"
            link_DDGs(trial_num_list, input_db_file, output_json_file, data_dir, script_name)

            print("Wrote workflow prov to " + output_json_file)

if __name__ == "__main__":
    main()

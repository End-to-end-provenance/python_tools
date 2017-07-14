import sqlite3
import json
import ast
import pandas
import os
import subprocess
import sys

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

def get_script_line_dict(script_name):
    """ keys = line_number, values = line
    used for labelling the process nodes """
    script_line_dict = {}

    with open(script_name) as f:
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
    to ensure correct sequential layout of DDG """

    current_informs_edge = {}
    current_informs_edge['prov:informant'] = prev_p
    current_informs_edge['prov:informed'] = current_p

    ekey_string = "e" + str(e_count)
    e_count+=1

    result['wasInformedBy'][ekey_string] = current_informs_edge

    return e_count

def add_start_node(result, step, p_count, current_line=None):
    """ adds start node and edge for current step """

    # make node
    start_node_d = {}
    start_node_d['rdt:type'] = "Start"
    start_node_d["rdt:elapsedTime"] = "0.5"
    keys = ["rdt:scriptNum", "rdt:startLine", "rdt:startCol", "rdt:endLine", "rdt:endCol"]
    for key in keys:
        start_node_d[key] = "NA"

    # choose most descriptive label for the node
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
    """ makes Finish node so that the function or loop is collapsible """

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

def add_process(result, p_name, p_count, s, script_name, current_line):
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

def check_input_files_and_add(input_db_file, run_num, p_count, files, s):
    """ if input file not detected bc of lacking "with open() as f" format
    detect input file by using db info
    makes sure that the string is a file/path
    if file/path: add to files dict
    then use add_file in make_dict """

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()
    c.execute('SELECT trial_id, value, function_activation_id, name from object_value where trial_id = ? and function_activation_id = ? and name = ?', (run_num, s[1], "filepath_or_buffer"))
    temp = c.fetchone()

    filename = temp[1].strip("'")

    try: #find existing file from that line
        # TO DO
        files[p_count-2]['name']
        # p_count-2 since incremented twice
    except: # make new entry
        temp_dict = {'hash': None, 'name': filename, 'mode': 'r'}
        files[temp[2]]= temp_dict

def check_input_intermediate_value_and_add(input_db_file, run_num, d_count, e_count, p_count, result, s, prev_p, script_name, data_dir, int_values):
    """ if to_csv does not have incoming data node bc of lacking recent assignment
    checks if intermediate value already exists in db and in result
    if not, adds node and edge """

    initial_d_count = d_count

    db = sqlite3.connect(input_db_file, uri=True)
    c = db.cursor()
    c.execute('SELECT trial_id, value, function_activation_id, name from object_value where trial_id = ? and function_activation_id = ? and name = ?', (run_num, s[1], "self", ))
    temp = c.fetchone()

    try:
        if temp[1] in int_values:
            # if incoming value already exists, do not add anything
            return d_count, e_count, None, None
        else:
            # add data node
            lst = list(s)
            lst[3]=temp[1]
            s = tuple(lst)
            d_count, e_count, dkey_string = add_data(result, s, d_count, e_count, prev_p, script_name, data_dir, to_csv = True)
            # if d_count != initial_d_count:
            #     print("it was okay, read to move on")
            # else:
            #     print("after a loop, redo")
            return d_count, e_count, dkey_string, s[3]
    except:
        print("except")
        return d_count, e_count, None, None

def add_file_node(script, current_link_dict, d_count, result, data_dict):
    """ adds a file node, called by add_file """

    #make file node
    current_file_node = {}
    current_file_node['rdt:name'] = script
    current_file_node['rdt:type'] = "File"
    keys = ['rdt:scope', "rdt:fromEnv", "rdt:timestamp", "rdt:location"]
    values = ["undefined", "FALSE", "", ""]
    for i in range (0, len(keys)):
        current_file_node[keys[i]] = values[i]

    split_path_file = current_link_dict['name'].split("/")
    temp = current_link_dict['name'].strip('.')
    # for consistency, stip all leading .

    # then, add 2 ..
    # set value/relative path according to file's parent directory
    try:
        if split_path_file[1] == "results":
            current_file_node['rdt:value'] = ".." + temp
        elif split_path_file[1] == "data":
            current_file_node['rdt:value'] = ".." +temp
        else:
            # if not in data or results, put entire path
            current_file_node['rdt:value']= current_link_dict['name']

    # avoid errors if the file name is not a full path and put entire path
    except:
        current_file_node['rdt:value']= current_link_dict['name']

    # add file node
    dkey_string = "d" + str(d_count)
    d_count+=1
    result["entity"][dkey_string] = current_file_node

    # add to dict of edges to make connections b/w graphs
    data_dict[script] = dkey_string

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

    # choose the correct category based on mode: r --> used, w --> wasGeneratedBy
    if current_link_dict['mode'] == "r":
        result['used'][e_string] = current_edge_node
    else:
        result['wasGeneratedBy'][e_string] = current_edge_node
        # if file created, add to outfiles dict for linking graphs
        inner_dict = {'data_node_num': dkey_string, 'source': activation_id_to_p_string[s[1]], 'hash_out': h}
        outer_dict = {path_array[-1] : inner_dict}
        outfiles[first_step[2]] = outer_dict

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

    if len(outfiles.keys()) !=0:
    # if not first script, check to see if the file already has a node using name and hash
        for script in outfiles.keys():
                for outfile in outfiles[script]:
                    # if already seen
                    if outfile == path_array[-1] and outfiles[script][outfile]['hash_out']==h:
                        # do not add new node, but return d_key_string
                        dkey_string = data_dict[path_array[-1]]

        if dkey_string == -1: # if not seen yet, add node
            d_count, dkey_string = add_file_node(path_array[-1], current_link_dict, d_count, result, data_dict)

        # no matter if new or seen, add new dependent edge
        e_count = add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles)

    # if first script, add file nodes w/o checking existence
    else:
        d_count, dkey_string = add_file_node(path_array[-1], current_link_dict, d_count, result, data_dict)
        e_count = add_file_edge(current_p, dkey_string, e_count, current_link_dict, result, activation_id_to_p_string, s, h, path_array, first_step, outfiles)

    return d_count, e_count

def get_data_frame(s):
    """ tries to convert to data to df to save intermediate value and improve vis """
    df = None
    if s[3]!=None:

        y = s[3].split("\n")
        # figure out formatting using first line, last line, len(second_line)
        first_line = y[0].strip()
        last_line = y[-1].strip()
        second_line_list = None

        try:
            second_line = y[1].strip()
            second_line_list = second_line.split()
        except:
            # print("it only has 1 line")
            pass

        # convert to df if full or subsetted df

        if "Unnamed:" in first_line: # entire dataframe
            col_names = first_line.split()[1:]
            data = []
            for l in y[1:]:
                line = l.split()[1:]
                data.append(line)
            df = pandas.DataFrame(data, columns = col_names)

        elif "Name:" in last_line: # subset of dataframe
            col_names = []
            temp = last_line.split()
            for i in range (1, int(len(temp)/2), 2):
                col_names.append(temp[i].strip(","))
            data = []
            for l in y[:-1]:
                line = l.split()[1:]
                data.append(line)
            df = pandas.DataFrame(data, columns = col_names)

        elif "rows" in last_line: #if last line == dimensions
            col_names = []
            data = []
            num_cols = int(last_line.split()[3])

            # separate data and col_names rows
            for l in y[:-1]:
                try:
                    int(l.split()[0])
                    data.append(l)
                except:
                    col_names.append(l.split())

            # formatting col_names
            for c in col_names:
                if c[0]=="..":
                    col_names.remove(c)
                elif  "\\" in c:
                    c.remove("\\")

            final_col_names = []
            for c in col_names:
                for col in c:
                    final_col_names.append(col)

            # formatting data
            first_line_dict = {}
            for d in data:
                temp = d.split()
                if temp[0] in first_line_dict:
                    for elt in temp[1:]:
                        first_line_dict[temp[0]].append(elt)
                else:
                    first_line_dict[temp[0]] = temp[1:]

            final_data = []
            for elt in first_line_dict:
                if len(first_line_dict[elt]) == num_cols:
                    final_data.append(first_line_dict[elt])

                # if easily visualized, keep it.
                # TO DO: how to merge strings together, ie Trinidad & Tobago

            df = pandas.DataFrame(final_data, columns = final_col_names)

        elif second_line_list != None: # no other indicators available

            data = []
            final_data = []
            col_names = first_line.split()

            if "\\" in col_names:
                # labelled cols, unlabelled rows
                col_names.remove("\\")

                # formatting col_names
                for l in y[1:]:
                    try:
                        int(l.split()[0])
                        data.append(l)
                    except:
                        # labelled cols, labelled rows
                        # TO DO: when rows labelled with count, mean, std, etc.

                        temp = l.split()
                        for elt in temp:
                            col_names.append(elt)

                num_cols = len(col_names)

                # formatting data
                first_line_dict = {}
                for d in data:
                    temp = d.split()
                    if temp[0] in first_line_dict:
                        for elt in temp[1:]:
                            first_line_dict[temp[0]].append(elt)
                    else:
                        first_line_dict[temp[0]] = temp[1:]

                final_data = []
                for elt in first_line_dict:
                    if len(first_line_dict[elt]) == num_cols:
                        final_data.append(first_line_dict[elt])

            else: # unlabelled cols + rows
                if len(second_line_list) == 1 + len(col_names):
                    final_data = []
                    for l in y[1:]:
                        line = l.split()
                        final_data.append(line[1:])

            df = pandas.DataFrame(final_data)

            # df initialization failed, reset it to None so that a string is printed instead of an empty df
            if df.empty:
                df = None
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
        if process >= lowest_process_num:
            p_string = activation_id_to_p_string[process]
            target_processes.append(p_string)

    return target_processes

def int_data_to_process(dkey_string, process_string, e_count, result):
    """ adds edge from intermediate data node to dependent process node """

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

    # iterate through each line in the script
    for i in range (1, len(script_steps)):
        s = script_steps[i]

        # get the line of the script to use as the process node label
        try:
            current_line = script_line_dict[s[4]]
        except:
            current_line = " "

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

            # try:
            #     next_s = script_steps[i+1]
            #     # get the next line of the script
            #     next_line = script_line_dict[next_s[4]]
            # except:
            #     next_line = "None"

            # if current_line != next_line:

            # add normal process node
            p_count, current_p = add_process(result, s[2], p_count, s, script_name, current_line)

            # special case checks
            if "pandas.read_csv" in current_line:
                # if reading csv, ensure that input file is detected, even w/o "with open as f" syntax
                check_input_files_and_add(input_db_file, run_num, p_count, files, s)

            if "to_csv" in current_line:
                d_count, e_count, dkey_string, int_value = check_input_intermediate_value_and_add(input_db_file, run_num, d_count, e_count, p_count, result, s, prev_p, script_name, data_dir, int_values)
                # if return value and dkey_string exists, add info to data structures
                if dkey_string != None:
                    int_values.append(int_value)
                    lowest_process_num.append(s[1])
                    int_dkey_strings.append(dkey_string)

        # dict for use in get_arguments_from_sql
        activation_id_to_p_string[s[1]] = current_p

        # if current_line != next_line:

        # if process node reads or writes to file, add file nodes and edges
        if s[1] in files.keys():
            d_count, e_count = add_file(result, files, d_count, e_count, current_p, s, outfiles, script_steps[0], activation_id_to_p_string, data_dict)

        # if process node has return statement, make intermediate data node and edges
        if s[3] != "None":
            d_count, e_count, dkey_string = add_data(result, s, d_count, e_count, current_p, script_name, data_dir)

            # if return value and dkey_string exists, add info to data structures
            if dkey_string != None:
                int_values.append(s[3])
                lowest_process_num.append(s[1])
                int_dkey_strings.append(dkey_string)

        # add_informs_edge between all process nodes
        e_count = add_informs_edge(result, prev_p, current_p, e_count)
        prev_p = "p" + str(p_count-1)

        # if function, NOT LOOP, has ended on current step, add finish node
        if s[4] == function_stack[-1]:
            # get the function name
            func_name = end_funcs[s[4]]

            # add the finish node and pop from the stack
            current_p, p_count = add_end_node(result, p_count, func_name)
            process_stack.pop()
            function_stack.pop()

            # add informs edge between last process node in loop and the finish node of the loop
            e_count = add_informs_edge(result, prev_p, current_p, e_count)
            prev_p = "p" + str(p_count-1)

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

    return result, p_count, d_count, e_count, outfiles, current_p

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

    # for each trial, query and add to the result
    for trial_num in trial_num_list:
        script_steps, files, func_ends, end_funcs = get_info_from_sql(input_db_file, trial_num)
        loop_dict = get_loop_locations(script_name)
        script_line_dict = get_script_line_dict(script_name)
        result, p_count, d_count, e_count, outfiles, finish_node = make_dict(script_steps, files, input_db_file, trial_num, func_ends, end_funcs, p_count, d_count, e_count, outfiles, result, data_dict, finish_node, script_name, loop_dict, data_dir, script_line_dict)

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
            print("Wrote prov for " + script_name + " to " + script_path)

        # workflow results -->results dir with name script1.json
        else:
            output_json_file, data_dir = get_paths(script_path)
            c.execute('SELECT id, script from trial where id = ? ', (trial_num_list[-1], ))
            temp = c.fetchone()
            last_script_name = temp[1]
            output_json_file = "/Users/jen/Desktop/newNow/results/workflow_" + script_name+ "_to_" + last_script_name +".json"
            link_DDGs(trial_num_list, input_db_file, output_json_file, data_dir, script_name)

            print("Wrote prov to " + script_path)

if __name__ == "__main__":
    main()

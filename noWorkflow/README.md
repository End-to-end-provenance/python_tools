Depends on the project noworkflow, installed via pip.

https://pypi.python.org/pypi/noworkflow
https://github.com/gems-uff/noworkflow

sql_to_json.py
Queries the database .noworkflow/db.sqlite that noworkflow creates in the scripts directory and converts the information to Prov-JSON file that can be read by DDG Explorer.

trial_num_list = [x, y]
where x and y are trial numbers associated with runs/scripts in the noWorkflow database.
Different scripts can be collapsed using Start/Finish nodes.
input_db_file = "path/to/.noworkflow/db.sqlite"
output_json_file = "path/to/results/scriptname.json"
linkDDGs(trial_num_list, input_db_file, output_json_file)

import ast
import os
import subprocess
from aqua.logger import log_configure
from aqua.util import create_folder, ConfigPath

def get_script_info(file_path=__file__):
    """
    Get the name and path of the current Python script.

    Returns:
        tuple: A tuple containing the script name and script path.
    """
    script_name = os.path.basename(file_path)
    script_path = os.path.abspath(file_path)
    
    print("Script Name (external):", script_name)
    print("Script Path (external):", script_path)
    
    return script_name, script_path

def extract_function_and_imports(source_code, function_name):
    tree = ast.parse(source_code)
    extracted_code = []
    imports = set()

    def extract_node(node, depth):
        nonlocal imports
        if isinstance(node, ast.FunctionDef):
            extracted_code.append('\t' * depth + ast.get_source_segment(source_code, node))
            for n in node.body:
                extract_node(n, depth + 1)
        elif isinstance(node, ast.Import):
            imports.add(ast.get_source_segment(source_code, node))
        elif isinstance(node, ast.ImportFrom):
            imports.add(ast.get_source_segment(source_code, node))

    for node in tree.body:
        extract_node(node, 0)

    return imports, '\n'.join(extracted_code)

def extract_and_write_function(source_path=__file__, function_name=None):
    
    script_name, script_path = get_script_info()
    destination_path = script_path+'tmp_'+script_name
    print("Destination Path:", destination_path)
    
    with open(source_path, 'r') as source_file:
        source_code = source_file.read()

        # Extract the specified function, its dependencies, and imports
        imports, extracted_code = extract_function_and_imports(source_code, function_name)

        # Write the extracted code to a new script
        with open(destination_path, 'w') as destination_file:
            destination_file.write(f"#!/usr/bin/env python3\n\n")
            destination_file.write(f"# Import necessary modules\n")
            for imp in imports:
                destination_file.write(f"{imp}\n")
            destination_file.write(f"{extracted_code}")
    return destination_path
            
def make_executable(file_path):
    """
    Make a Python file executable by adding the execute permission.

    Args:
        file_path (str): Path to the Python file.

    Returns:
        None
    """
    try:
        # Get the current file permissions
        current_permissions = os.stat(file_path).st_mode

        # Add the execute permission for the owner
        new_permissions = current_permissions | 0o100

        # Set the new file permissions
        os.chmod(file_path, new_permissions)

        print(f"File '{file_path}' is now executable.")
    except Exception as e:
        print(f"Error making the file executable: {e}")

def remove_file(file_path=None):
    """
    Remove a file.

    Args:
        file_path (str): Path to the file to be removed.

    Returns:
        bool: True if the file was successfully removed, False otherwise.
    """
    try:
        os.remove(file_path)
        print(f"File '{file_path}' removed successfully.")
        return True
    except Exception as e:
        print(f"Error removing the file '{file_path}': {e}")
        return False

def output_dir(path_to_output='.', loglevel='WARNING'):
    """
    Creating the directory for output if it does not exist

    Args:
        path_to_output (str, optional): The path to the directory,
                                        which will contain logs/errors and
                                        output of Slurm Jobs. Defaults is '.'
        loglevel (str, optional):       The level of logging.
                                        Defaults to 'WARNING'.

    Returns:
        logs_path (str):    The path to the directory for logs/errors
        output_path (str):  The path to the directory for output
    """
    logs_path = str(path_to_output)+"/slurm/logs"
    output_path = str(path_to_output)+"/slurm/output"

    # Creating the directory for logs and output
    create_folder(folder=str(path_to_output)+"/slurm", loglevel=loglevel)
    create_folder(folder=logs_path, loglevel=loglevel)
    create_folder(folder=output_path, loglevel=loglevel)

    return logs_path, output_path

def submit_slurm_job(script_path_func, job_name=None, path_to_output=None, memory=None, queue=None,
                     walltime=None, nodes=None, cores=None, account=None, loglevel='WARNING'):

    # Creating the directory for logs and output
    logs_path, output_path = output_dir(path_to_output=path_to_output,
                                        loglevel=loglevel)
    slurm_command = [
        "sbatch",
        "--job-name", job_name,
        "--output", output_path,
        "--error", logs_path,
        "--time", walltime,
        "--nodes", str(nodes),
        "--ntasks-per-node", str(cores),
        "--mem", memory.replace(" ", ""),  # Specify the amount of memory (adjust the value accordingly)
        "--partition", queue,  # Specify the name of the queue
        "--account", account,  # Specify the account
        script_path_func
    ]

    subprocess.run(slurm_command)

def job(source_path, function_name=None, job_name='slurm', path_to_output='.', queue=None, account=None,
        configdir=None, walltime="2:30:00", nodes=1, cores=1, memory="10 GB", machine=None, loglevel='WARNING'):
    
    logger = log_configure(log_level=loglevel, log_name='slurm')
    
    if machine is None:
        Configurer = ConfigPath(configdir=configdir)
        machine_name = Configurer.machine
    else:
        machine_name = machine
        
    if queue is None:
        if machine_name == "levante":
            queue = "compute"
        elif machine_name == "lumi":
            queue = "small"
        else:
            raise Exception("The queue is not defined. Please, define the queue manually.")
    if account is None:
        if machine_name == "levante":
            account = "bb1153"
        elif machine_name == "lumi":
            account = "project_465000454"
        else:
            raise Exception("The account is not defined. Please, define the account manually.")
        
    if function_name is not None:
        destination_path = extract_and_write_function(source_path=source_path, function_name=function_name) 
        source_path = destination_path
    
    make_executable(source_path)
        
    submit_slurm_job(script_path_func=source_path, job_name=job_name, path_to_output=path_to_output, account=account,
                     memory=memory, queue=queue, walltime=walltime, nodes=nodes, cores=cores, loglevel=loglevel)
    
    #if function_name is not None
    #    remove_file(file_path=destination_path)
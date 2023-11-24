import ast
import os
import subprocess
from aqua.logger import log_configure
from aqua.util import create_folder, ConfigPath

log_level = 'WARNING'
logger = log_configure(log_level=log_level, log_name='slurm')

def get_script_info(file_path=__file__, log_level='WARNING'):
    """
    Get information about the current Python script.

    Args:
        file_path (str, optional): The path to the Python script. Defaults to the current script (__file__).
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        tuple: A tuple containing the script name and directory path.
    """
    logger = log_configure(log_level=log_level, log_name='slurm')
    
    script_name = os.path.basename(file_path)
    script_path = os.path.abspath(file_path)
    script_directory = os.path.dirname(script_path)
    
    logger.debug(f"Script Name (external): {script_name}")
    logger.debug(f"Script Path (external): {script_path}")
    logger.debug(f"Script Directory (external): {script_directory}")
    
    return script_name, script_directory

def extract_function_and_imports(source_code, function_name, log_level='WARNING'):
    """
    Extracts the specified function and associated imports from the given source code.

    Args:
        source_code (str): The source code containing the target function and imports.
        function_name (str): The name of the function to extract.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        tuple: A tuple containing a set of imports and the extracted code of the specified function.

    """
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

def extract_and_write_function(source_path=__file__, function_name=None, log_level='WARNING'):
    """
    Extracts the specified function and its dependencies from a source code file,
    writes them to a new script, and returns the path of the new script.

    Args:
        source_path (str, optional): The path to the source code file. Defaults to the current script (__file__).
        function_name (str, optional): The name of the function to extract. Defaults to None.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        str: The path of the newly created script containing the extracted function and imports.
    """
    script_name, script_directory = get_script_info(log_level=log_level)
    destination_path = script_directory+'/tmp_'+script_name
    logger.debug(f"Destination Path: {destination_path}")
    
    with open(source_path, 'r') as source_file:
        source_code = source_file.read()

        # Extract the specified function, its dependencies, and imports
        imports, extracted_code = extract_function_and_imports(source_code=source_code, function_name=function_name, log_level=log_level)

        # Write the extracted code to a new script
        with open(destination_path, 'w') as destination_file:
            destination_file.write(f"#!/usr/bin/env python3\n\n")
            destination_file.write(f"# Import necessary modules\n")
            for imp in imports:
                destination_file.write(f"{imp}\n")
            destination_file.write(f"{extracted_code}")
    return destination_path
            
def make_executable(file_path, log_level='WARNING'):
    """
    Make a Python file executable by adding the execute permission.

    Args:
        file_path (str): Path to the Python file.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

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

        logger.debug(f"File '{file_path}' is now executable.")
    except Exception as e:
        logger.debug(f"Error making the file executable: {e}")

def remove_file(file_path=None, log_level='WARNING'):
    """
    Remove a file.

    Args:
        file_path (str): Path to the file to be removed.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        bool: True if the file was successfully removed, False otherwise.
    """
    try:
        os.remove(file_path)
        logger.debug(f"File '{file_path}' removed successfully.")
        return True
    except Exception as e:
        logger.debug(f"Error removing the file '{file_path}': {e}")
        return False

def output_dir(path_to_output='.', log_level='WARNING'):
    """
    Create directories for logs and output if they do not exist.

    Args:
        path_to_output (str, optional): The path to the directory that will contain logs/errors and
                                        output of Slurm Jobs. Defaults to '.'.
        log_level (str, optional): The logging level. Defaults to 'WARNING'.

    Returns:
        tuple: A tuple containing the paths to the directory for logs/errors and the directory for output.
    """
    logs_path = str(path_to_output)+"/slurm/logs"
    output_path = str(path_to_output)+"/slurm/output"

    # Creating the directory for logs and output
    create_folder(folder=str(path_to_output)+"/slurm", loglevel=log_level)
    create_folder(folder=logs_path, loglevel=log_level)
    create_folder(folder=output_path, loglevel=log_level)

    return logs_path, output_path

def submit_slurm_job(script_path_func, job_name=None, path_to_output=None, memory=None, queue=None,
                     walltime=None, nodes=None, cores=None, account=None, log_level='WARNING'):
    """Submit a job to SLURM (Simple Linux Utility for Resource Management).

    Args:
        script_path_func (str): Path to the Python script to be executed.
        job_name (str, optional): Name of the SLURM job. Defaults to None.
        path_to_output (str, optional): Path to the directory for logs and output. Defaults to None.
        memory (str, optional): Amount of memory required for the job. Defaults to None.
        queue (str, optional): Name of the SLURM queue. Defaults to None.
        walltime (str, optional): Maximum execution time for the job (HH:MM:SS). Defaults to None.
        nodes (int, optional): Number of nodes for the job. Defaults to None.
        cores (int, optional): Number of CPU cores per node for the job. Defaults to None.
        account (str, optional): Account associated with the job. Defaults to None.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        None
    """
    # Creating the directory for logs and output
    logs_path, output_path = output_dir(path_to_output=path_to_output,
                                        log_level=log_level)
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
        configdir=None, walltime="2:30:00", nodes=1, cores=1, memory="10 GB", machine=None, log_level='WARNING'):
    """
    Submit a job to SLURM (Simple Linux Utility for Resource Management).

    Args:
        source_path (str): Path to the Python script to be executed or the source file containing the specified function.
        function_name (str, optional): Name of the function to extract and submit as a separate job. Defaults to None.
        job_name (str, optional): Name of the SLURM job. Defaults to 'slurm'.
        path_to_output (str, optional): Path to the directory for logs and output. Defaults to '.'.
        queue (str, optional): Name of the SLURM queue. Defaults to None.
        account (str, optional): Account associated with the job. Defaults to None.
        configdir (str, optional): Path to the directory containing configurations. Defaults to None.
        walltime (str, optional): Maximum execution time for the job (HH:MM:SS). Defaults to "2:30:00".
        nodes (int, optional): Number of nodes for the job. Defaults to 1.
        cores (int, optional): Number of CPU cores per node for the job. Defaults to 1.
        memory (str, optional): Amount of memory required for the job. Defaults to "10 GB".
        machine (str, optional): Name of the machine where the job will be executed. Defaults to None.
        log_level (str, optional): The logging level for configuring the logger. Defaults to 'WARNING'.

    Returns:
        None
    """
    if machine is None:
        Configurer = ConfigPath(configdir=configdir)
        machine_name = Configurer.machine
    else:
        machine_name = machine
        
    logger.debug(f"Machine Name: {machine_name}")
    
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
    logger.debug(f"Queue: {queue}")
    logger.debug(f"Account: {account}")
    if function_name is not None:
        destination_path = extract_and_write_function(source_path=source_path, function_name=function_name, log_level=log_level) 
        source_path = destination_path
    
    make_executable(source_path, log_level=log_level)
        
    submit_slurm_job(script_path_func=source_path, job_name=job_name, path_to_output=path_to_output, account=account,
                     memory=memory, queue=queue, walltime=walltime, nodes=nodes, cores=cores, log_level=log_level)
    
    if function_name is not None:
        remove_file(file_path=destination_path)
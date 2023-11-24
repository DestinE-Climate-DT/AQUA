import ast
import os
import subprocess

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
# Example usage
#script_name, script_path = get_script_info()

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

def extract_and_write_function(source_path=__file__, function_name=None, destination_path=None):
    
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
# Example usage
#extract_and_write_function("/work/bb1153/b382267/AQUA/cli/weights/generate_weights_for_catalog.py", "generate_catalogue_weights_on_slurm", "new_script.py")

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
# Example usage
# make_executable("/work/bb1153/b382267/AQUA/aqua/slurm/new_script.py")

def submit_slurm_job(script_path_func, job_name="my_job", output_file="my_job.out", error_file="my_job.err", time="24:00:00", nodes=1, tasks_per_node=1):
    # Get the Python script path by calling the provided function
    python_script_path = script_path_func()

    slurm_command = [
        "sbatch",
        "--job-name", job_name,
        "--output", output_file,
        "--error", error_file,
        "--time", time,
        "--nodes", str(nodes),
        "--ntasks-per-node", str(tasks_per_node),
        python_script_path
    ]

    subprocess.run(slurm_command)

# extract_and_write_function(source_path=__file__, function_name=None, destination_path=None)
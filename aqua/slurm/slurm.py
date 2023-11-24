import ast
import os

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

def extract_and_write_function(source_path, function_name, destination_path):
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

def get_script_info():
    """
    Get the name and path of the current Python script.

    Returns:
        tuple: A tuple containing the script name and script path.
    """
    script_name = os.path.basename(__file__)
    script_path = os.path.abspath(__file__)
    return script_name, script_path

# Example usage
# script_name, script_path = get_script_info()
# print("Script Name:", script_name)
# print("Script Path:", script_path)

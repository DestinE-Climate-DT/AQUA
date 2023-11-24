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

# Get the name and path of the current Python script
script_name = os.path.basename(__file__)
script_path = os.path.abspath(__file__)

print("Script Name:", script_name)
print("Script Path:", script_path)
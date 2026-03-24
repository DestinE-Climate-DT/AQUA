""" Utility to parse a yaml file from bash """

import sys
import yaml
import os

def get_nested(data, keys):
    """
    Get a nested value from a dictionary or list using a list of keys.
    """
    for key in keys:
        if isinstance(data, dict):
            data = data.get(key)
        elif isinstance(data, list):
            try:
                key = int(key)
                data = data[key]
            except (ValueError, IndexError):
                return None
        else:
            return None
        if data is None:
            return None
    return data

def parse_yaml(query, file_path):
    # Load the YAML file
    with open(file_path, 'r') as file:
        data = yaml.safe_load(file)

    # Split the query into keys
    keys = query.strip('.').split('.')

    # Get the nested value
    return get_nested(data, keys)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python parse_yaml.py '<query>' <file_path>")
        sys.exit(1)

    query = sys.argv[1]
    file_path = sys.argv[2]

    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        sys.exit(1)

    result = parse_yaml(query, file_path)
    if result is not None:
        if isinstance(result, dict):
            print(yaml.dump(result, default_flow_style=False))
        elif isinstance(result, list):
            for item in result:
                print(item, end=' ')

        else:
            print(result)
    else:
        print("")

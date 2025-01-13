#!/usr/bin/env python

# This script creates content.yaml and content.json files for each experiment in the content/png directory.

import os
import sys
import yaml
import json
import argparse
from pypdf import PdfReader

def make_content(catalog, model, exp, diagnostics, experiments, force):
    """
    Create content.yaml and content.json files for a specific experiment.

    Args:
        catalog (str): The catalog name
        model (str): The model name
        exp (str): The experiment name
        diagnostics (dict): A dictionary of diagnostics and their groupings
        experiments (dict): A dictionary of experiment descriptions
        force (bool): Create content.yaml and content.json even if they exist already
    """

    if (not os.path.exists(f"{catalog}/{model}/{exp}/content.yaml") or 
        not os.path.exists(f"{catalog}/{model}/{exp}/content.json") or force):

        # Retrieve the date of the latest commit
        infile = f"../pdf/{catalog}/{model}/{exp}/last_update.txt"
        # check if file "infile" exists
        if os.path.exists(infile):
            with open(infile, "r") as file:
                date_time = file.read().strip()
        else:
            date_time = None

        exp_description = experiments.get(f"{catalog}_{model}_{exp}")
        if exp_description:
            title = exp_description.get("title", f"{exp}")         
            desc = exp_description.get("description", "")
            note = exp_description.get("note", "")

        content = {}
        content['experiment'] = {"catalog": catalog, "model": model, "experiment": exp}
        if title:
            content['experiment']['title'] = title
        if desc:
            content['experiment']['description'] = desc
        if note:
            content['experiment']['note'] = note
        if date_time:
            content['experiment']['last_update'] = date_time

        filename_list = []
        properties = {}

        for fn in os.listdir(f"{catalog}/{model}/{exp}"):
            if fn.endswith(".png"):
                fn_line = f"{catalog}/{model}/{exp}/{fn}"
                filename_list.append(fn_line)
                # Read description for capion from the pdf file
                pdf_path = f"../pdf/{catalog}/{model}/{exp}/" + os.path.splitext(fn)[0] + ".pdf"
                pdf_reader = PdfReader(pdf_path)
                metadata = pdf_reader.metadata
                properties[fn_line] = metadata

        grouping = {}
        for key, val in diagnostics.items():
            if isinstance(val, list):
                grouping[key] = [fn for fn in filename_list for v in val if v in fn]
            else:
                grouping[key] = [fn for fn in filename_list if val in fn]

        # Add other diagnostics to the grouping
        diagnostics_vals = []
        for key, val in diagnostics.items():
            if isinstance(val, str):
                diagnostics_vals.append(val)
            elif isinstance(val, list):
                diagnostics_vals.extend(val)

        other_group = [fn for fn in filename_list if all(val not in fn for val in diagnostics_vals)]
        if other_group:
            grouping["Other diagnostics"] = other_group

        content['files'] = {}
        content['diagnostics'] = []
        for key, val in grouping.items():
            if val:
                content['diagnostics'].append(key)
            for v in val:
                content['files'][v] = {'grouping': str(key)}
                prop = properties.get(v, {})
                if prop:
                    for kk, vv in prop.items():
                        keystr = str(kk).lstrip('/').lower()
                        content['files'][v][keystr] = str(vv)

        with open(f"{catalog}/{model}/{exp}/content.yaml", "w") as file:
            yaml.dump(content, file, default_style='"')

        # Convert content to JSON
        content_json = json.dumps(content, indent=4)

        # Write content JSON to file
        with open(f"{catalog}/{model}/{exp}/content.json", "w") as file:
            file.write(content_json)


def main(force=False, experiment=None):
    """
    Main function to create content.yaml and content.json files for each experiment in the content/png directory.

    Args:
        force (bool): Create content.yaml and content.json even if they exist already
    """
        
    with open("config.yaml", "r") as file:
        config = yaml.safe_load(file)

    os.chdir("content/png")

    diagnostics = config["diagnostics"]
    experiments = config["experiments"]

    if experiment:
        catalog, model, exp = experiment.split('/')
        make_content(catalog, model, exp, diagnostics, experiments, force)
    else:
        for catalog in os.listdir("."):
            for model in os.listdir(f"./{catalog}"):
                for exp in os.listdir(f"./{catalog}/{model}"): 
                    make_content(catalog, model, exp, diagnostics, experiments, force)
                    

def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='Create content.yaml and content.json files for each experiment in the content/png directory.')

    parser.add_argument('-f', '--force', action="store_true",
                        help='create content.yaml and content.json even if they exist already')
    parser.add_argument('-e', '--experiment', type=str,
                        help='specific experiment for which to create content')
    
    return parser.parse_args(arguments)
    

if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    force = args.force
    experiment = args.experiment
    main(force=force, experiment=experiment)

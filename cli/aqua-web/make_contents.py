#!/usr/bin/env python

# This script creates content.yaml and content.json files for each experiment in the content/png directory.

import os
import sys
import yaml
import json
import argparse
from pypdf import PdfReader


def deep_merge_dicts(d1, d2):
    """
    Merge two dictionaries modifying the first one.
    Values are assumed to be either dictionaries or lists.
    """

    for key, val in d2.items():
        if key not in d1:
            d1[key] = val
        else:
            if isinstance(d1[key], dict) and isinstance(val, dict):
                deep_merge_dicts(d1[key], val)
            elif isinstance(d1[key], list):
                # Ensure val behaves like a list
                items = val if isinstance(val, list) else [val]
                for item in items:
                    if item not in d1[key]:
                        d1[key].append(item)
            else:
                d1[key] = val


def has_valid_key(d, key):
    return key in d and d[key] is not None


def make_content(catalog, model, exp, diagnostics, config_experiments, force):
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

    # Update or create experiments.yaml
    if not os.path.exists("../experiments.yaml"):
        experiments = {catalog: {model: [exp]}}
    else:
        with open("../experiments.yaml", "r") as file:
            experiments = yaml.safe_load(file)
        add = {catalog: {model: [exp]}}
        deep_merge_dicts(experiments, add)

    # Place the file at the "./contents" level
    with open("../experiments.yaml", "w") as file:
        yaml.dump(experiments, file, default_flow_style=False)

    # Make content.yaml/json
    if (not os.path.exists(f"{catalog}/{model}/{exp}/content.yaml") or 
        not os.path.exists(f"{catalog}/{model}/{exp}/content.json") or force):

        if os.path.exists(f"{catalog}/{model}/{exp}/experiment.yaml"):
            with open(f"{catalog}/{model}/{exp}/experiment.yaml", "r") as file:
                experiment = yaml.safe_load(file)

                if "dashboard" in experiment:
                    experiment.update(experiment.pop("dashboard"))

                # rename legacy keys
                if "exp" in experiment:
                    experiment["experiment"] = experiment.pop("exp")
                if "expver" in experiment:
                    experiment["expid"] = experiment.pop("expver")
                if "resolution" in experiment:
                    experiment["resolution_id"] = experiment.pop("resolution")

                # Build title based on resolution and expid if available
                if has_valid_key(experiment, "menu"):
                    experiment["title"] = experiment["menu"]
                else:
                    experiment["title"] = exp
                
                info_parts = []
                if has_valid_key(experiment, "resolution_id"):
                    info_parts.append(experiment["resolution_id"])
                if has_valid_key(experiment, "expid"):
                    info_parts.append(experiment["expid"])
                if info_parts:
                    experiment["title"] += f" ({','.join(info_parts)})"

        else:
            # No experiment file. Check if some info is still in the config.yaml (legacy, will be removed)
            
            exp_metadata = config_experiments.get(f"{catalog}_{model}_{exp}")
            experiment = {"catalog": catalog, "model": model, "experiment": exp}

            if exp_metadata:
                experiment['title'] = exp_metadata.get("title", f"{exp}")      
                experiment['description'] = exp_metadata.get("description", "")
                experiment['note'] = exp_metadata.get("note", "")

        if "title" in experiment:  # Duplicate to future proof (ultimately we want to switch to 'label')
            experiment["label"] = experiment["title"]

        # Retrieve the date of the latest commit
        infile = f"../pdf/{catalog}/{model}/{exp}/last_update.txt"
        if os.path.exists(infile):
            with open(infile, "r") as file:
                experiment['last_update'] = file.read().strip()     

        content = {}
        content['experiment'] = experiment
        
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
    config_experiments = config.get("experiments", {})

    if experiment:
        catalog, model, exp = experiment.split('/')
        make_content(catalog, model, exp, diagnostics, config_experiments, force)
    else:
        for catalog in os.listdir("."):
            for model in os.listdir(f"./{catalog}"):
                for exp in os.listdir(f"./{catalog}/{model}"): 
                    make_content(catalog, model, exp, diagnostics, config_experiments, force)
                    

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

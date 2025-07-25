#!/usr/bin/env python

# This script creates content.yaml and content.json files for each experiment in the content/png directory.

import os
import sys
import yaml
import json
import argparse
import logging
from pypdf import PdfReader

# Get a logger instance
logger = logging.getLogger('make_contents')


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


def convert_to_new_structure(data):
    """
    Converts the experiments dictionary from a 3-level structure (list of experiments)
    to a 4-level structure (dictionary of experiments with a list of realizations).
    If the structure is already 4-level, it remains unchanged.
    """
    for catalog, models in data.items():
        if isinstance(models, dict):
            for model, experiments in models.items():
                if isinstance(experiments, list):
                    # Old structure found: {model: [exp1, exp2]}
                    new_experiments_dict = {exp: ['r1'] for exp in experiments}
                    # Replace the list with the new dictionary
                    models[model] = new_experiments_dict
    return data


def has_valid_key(d, key):
    return key in d and d[key] is not None


def make_content(catalog, model, exp, realization, diagnostics, config_experiments, force):
    """
    Create content.yaml and content.json files for a specific experiment.

    Args:
        catalog (str): The catalog name
        model (str): The model name
        exp (str): The experiment name
        realization (str): The realization name (optional, can be None)
        diagnostics (dict): A dictionary of diagnostics and their groupings
        experiments (dict): A dictionary of experiment descriptions
        force (bool): Create content.yaml and content.json even if they exist already
    """

    # Update or create experiments.yaml
    if not os.path.exists("../experiments.yaml"):
        if realization:
            experiments = {catalog: {model: {exp: [realization]}}}
        else:
            experiments = {catalog: {model: [exp]}}
    else:
        with open("../experiments.yaml", "r") as file:
            experiments = yaml.safe_load(file)

        if realization:
            add = {catalog: {model: {exp: [realization]}}}           
            experiments = convert_to_new_structure(experiments)  # Convert to new structure if necessary
        else:
            add = {catalog: {model: [exp]}}

        deep_merge_dicts(experiments, add)

    # Place the file at the "./contents" level
    with open("../experiments.yaml", "w") as file:
        yaml.dump(experiments, file, default_flow_style=False)

    # Make content.yaml/json

    path = f"{catalog}/{model}/{exp}"
    if realization:
        path += f"/{realization}"

    if (not os.path.exists(f"{path}/content.yaml") or 
        not os.path.exists(f"{path}/content.json") or force):
        
        logger.debug(f"Generating content files for {path} (force={force})")

        if os.path.exists(f"{path}/experiment.yaml"):
            logger.debug(f"Found experiment.yaml for {path}")
            with open(f"{path}/experiment.yaml", "r") as file:
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
                if not has_valid_key(experiment, "realization"):  # if realization already spoecified in experiment.yaml use that one
                    experiment["realization"] = realization if realization else "r1"

        else:
            # No experiment file. Check if some info is still in the config.yaml (legacy, will be removed)
            logger.warning(f"No experiment.yaml found for {path}. Using legacy info from config.")
            exp_metadata = config_experiments.get(f"{catalog}_{model}_{exp}")
            experiment = {"catalog": catalog, "model": model, "experiment": exp}
            experiment["realization"] = realization if realization else "r1"  # Default realization if not specified, does not harm

            if exp_metadata:
                experiment['title'] = exp_metadata.get("title", f"{exp}")      
                experiment['description'] = exp_metadata.get("description", "")
                experiment['note'] = exp_metadata.get("note", "")

        if "title" in experiment:  # Duplicate to future proof (ultimately we want to switch to 'label')
            experiment["label"] = experiment["title"]

        # Retrieve the date of the latest commit
        infile = f"../pdf/{path}/last_update.txt"
        if os.path.exists(infile):
            with open(infile, "r") as file:
                experiment['last_update'] = file.read().strip()     
        else:
            logger.debug(f"last_update.txt not found at {infile}")

        content = {}
        content['experiment'] = experiment
        
        filename_list = []
        properties = {}

        for fn in os.listdir(f"{path}"):
            if fn.endswith(".png"):
                if realization:
                    fn_line = f"{catalog}/{model}/{exp}/{realization}/{fn}"
                else:
                    fn_line = f"{catalog}/{model}/{exp}/{fn}"
                filename_list.append(fn_line)
                # Read description for capion from the pdf file
                pdf_path = f"../pdf/{path}/" + os.path.splitext(fn)[0] + ".pdf"
                if os.path.exists(pdf_path):
                    pdf_reader = PdfReader(pdf_path)
                else:
                    logger.warning(f"Missing corresponding PDF file for {fn} at {pdf_path}")
                    continue  # If the PDF does not exist, skip this file
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

        with open(f"{path}/content.yaml", "w") as file:
            yaml.dump(content, file, default_style='"')
        logger.debug(f"Successfully wrote {path}/content.yaml")

        # Convert content to JSON
        content_json = json.dumps(content, indent=4)

        # Write content JSON to file
        with open(f"{path}/content.json", "w") as file:
            file.write(content_json)
        logger.debug(f"Successfully wrote {path}/content.json")

    else:
        logger.info(f"Content files for {path} already exist. Skipping. Use --force to overwrite.")


def main(force=False, experiment=None, configfile="config.yaml", ensemble=True, loglevel="INFO"):
    """
    Main function to create content.yaml and content.json files for each experiment in the content/png directory.

    Args:
        force (bool): Create content.yaml and content.json even if they exist already
        ensemble (bool): If True, assumes a new structure with 4 levels (catalog/model/experiment/realization).
        experiment (str): Specific experiment for which to create content (in format "$catalog/$model/$experiment")
        configfile (str): Alternate confg file path (default "config.yaml" - used by aqua-web)
        loglevel (str): The logging level to use (e.g., 'INFO', 'DEBUG').
    """
    
    # Configure logging
    logger.setLevel(loglevel.upper())
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s :: %(name)s :: %(levelname)s -> %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
        
    logger.info(f"Starting content generation with log level {loglevel}")
    try:
        with open(configfile, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {configfile}")
        sys.exit(1)

    os.chdir("content/png")
    logger.info(f"Changed directory to {os.getcwd()}")

    diagnostics = config.get("diagnostics", {})
    if not diagnostics:
        logger.warning("No 'diagnostics' section found in config file.")
    config_experiments = config.get("experiments", {})

    if experiment:
        logger.info(f"Processing single experiment: {experiment}")
        parts = experiment.split('/')

        if len(parts) == 3:
            catalog, model, exp = parts
            realization = None
        elif len(parts) == 4:
            catalog, model, exp, realization = parts
        else:
            raise ValueError("Experiment must be in the format $catalog/$model/$experiment[/realization]")

        make_content(catalog, model, exp, realization, diagnostics, config_experiments, force)
    else:  # run through all subdirs
        logger.info("Processing all subdirectories...")
        if os.path.exists("../experiments.yaml"):  # Let's start fresh
            logger.info("Removing existing experiments.yaml to start fresh.")
            os.remove("../experiments.yaml")

        for catalog in os.listdir("."):
            catalog_path = os.path.join(".", catalog)
            if not os.path.isdir(catalog_path): continue
            logger.debug(f"Scanning catalog: {catalog}")
            for model in os.listdir(catalog_path):
                model_path = os.path.join(catalog_path, model)
                if not os.path.isdir(model_path): continue
                logger.debug(f"Scanning model: {model}")
                for exp in os.listdir(model_path):
                    exp_path = os.path.join(model_path, exp)
                    if not os.path.isdir(exp_path): continue
                    logger.debug(f"Scanning experiment: {exp}")
                    
                    if ensemble:  # If new structure, iterate through 4 levels
                        for realization in os.listdir(exp_path):
                            realization_path = os.path.join(exp_path, realization)
                            if not os.path.isdir(realization_path): continue
                            logger.debug(f"Processing ensemble member: {catalog}/{model}/{exp}/{realization}")
                            make_content(catalog, model, exp, realization, diagnostics, config_experiments, force)
                    else:
                        logger.debug(f"Processing experiment: {catalog}/{model}/{exp}")
                        make_content(catalog, model, exp, None, diagnostics, config_experiments, force)
                    

def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='Create content.yaml and content.json files for each experiment in the content/png directory.')

    parser.add_argument('-n', '--no-ensemble', action="store_false",
                        dest='ensemble',
                        help='When processing all subdirectories, revert to old structure with 3 levels (catalog/model/experiment) instead of default 4 levels (catalog/model/experiment/realization).')
    parser.add_argument('-f', '--force', action="store_true",
                        help='Create content.yaml and content.json even if they exist already')
    parser.add_argument('-e', '--experiment', type=str,
                        help='Specific experiment for which to create content in format $catalog/$model/$experiment/$realization. Realization is optional.')
    parser.add_argument('-c', '--config', type=str, default="config.yaml",
                        help='Alternate config file')
    parser.add_argument('-l', '--loglevel', type=str, default='INFO',
                        help='Set the logging level (e.g., DEBUG, INFO, WARNING). Default is INFO.')
    
    return parser.parse_args(arguments)
    

if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    force = args.force
    experiment = args.experiment
    config = args.config
    ensemble = args.ensemble
    loglevel = args.loglevel
    main(force=force, experiment=experiment, configfile=config, ensemble=ensemble, loglevel=loglevel)

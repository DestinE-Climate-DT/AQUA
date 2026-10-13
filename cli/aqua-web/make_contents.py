#!/usr/bin/env python

# This script creates content.yaml and content.json files for each experiment in the content/png directory.

import argparse
import json
import logging
import os
import sys
import xml.etree.ElementTree as ET
from fnmatch import fnmatch
from pathlib import Path

import yaml
from PIL import Image
from pypdf import PdfReader

# Get a logger instance
logger = logging.getLogger("make_contents")


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
    if data:
        for catalog, models in data.items():
            if isinstance(models, dict):
                for model, experiments in models.items():
                    if isinstance(experiments, list):
                        # Old structure found: {model: [exp1, exp2]}
                        new_experiments_dict = {exp: ["r1"] for exp in experiments}
                        # Replace the list with the new dictionary
                        models[model] = new_experiments_dict
    return data


def has_valid_key(d, key):
    return key in d and d[key] is not None


def make_content(catalog, model, exp, realization, diagnostics, config_experiments, force, content_dir=Path(".")):
    """
    Create content.yaml and content.json files for a specific experiment.

    Args:
        catalog (str): The catalog name
        model (str): The model name
        exp (str): The experiment name
        realization (str): The realization name (optional, can be None)
        diagnostics (dict): A dictionary of diagnostics and their groupings
        config_experiments (dict): A dictionary of experiment descriptions
        force (bool): Create content.yaml and content.json even if they exist already
        content_dir (Path): Directory containing experiment folders, defaulting to the current directory.
    """

    # Update or create experiments.yaml
    content_dir = Path(content_dir)
    experiments_file = content_dir / ".." / "experiments.yaml"
    if not experiments_file.exists():
        if realization:
            experiments = {catalog: {model: {exp: [realization]}}}
        else:
            experiments = {catalog: {model: [exp]}}
    else:
        with experiments_file.open("r") as file:
            experiments = yaml.safe_load(file)

        if realization:
            add = {catalog: {model: {exp: [realization]}}}
            experiments = convert_to_new_structure(experiments)  # Convert to new structure if necessary
        else:
            add = {catalog: {model: [exp]}}

        deep_merge_dicts(experiments, add)

    # Place the file at the "./contents" level
    with experiments_file.open("w") as file:
        yaml.dump(experiments, file, default_flow_style=False)

    # Make content.yaml/json

    path = f"{catalog}/{model}/{exp}"
    if realization:
        path += f"/{realization}"

    experiment_dir = content_dir / path
    if not (experiment_dir / "content.yaml").exists() or not (experiment_dir / "content.json").exists() or force:
        logger.debug(f"Generating content files for {path} (force={force})")

        if (experiment_dir / "experiment.yaml").exists():
            logger.debug(f"Found experiment.yaml for {path}")
            with (experiment_dir / "experiment.yaml").open("r") as file:
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
                # If realization is already specified in experiment.yaml, keep it.
                if not has_valid_key(experiment, "realization"):
                    experiment["realization"] = realization if realization else "r1"

        else:
            # No experiment file. Check if some info is still in the config.yaml (legacy, will be removed)
            logger.warning(f"No experiment.yaml found for {path}. Using legacy info from config.")
            exp_metadata = config_experiments.get(f"{catalog}_{model}_{exp}")
            experiment = {"catalog": catalog, "model": model, "experiment": exp}
            # Default realization if not specified, does not harm
            experiment["realization"] = realization if realization else "r1"

            if exp_metadata:
                experiment["title"] = exp_metadata.get("title", f"{exp}")
                experiment["description"] = exp_metadata.get("description", "")
                experiment["note"] = exp_metadata.get("note", "")

        if "title" in experiment:  # Duplicate to future proof (ultimately we want to switch to 'label')
            experiment["label"] = experiment["title"]

        # Retrieve the date of the latest commit
        infile = experiment_dir / "last_update.txt"
        if infile.exists():
            with infile.open("r") as file:
                experiment["last_update"] = file.read().strip()
        else:
            logger.debug(f"File {infile} not found.")

        content = {}
        content["experiment"] = experiment

        filename_list = []
        properties = {}

        for fn in os.listdir(experiment_dir):
            if realization:
                fn_line = f"{catalog}/{model}/{exp}/{realization}/{fn}"
            else:
                fn_line = f"{catalog}/{model}/{exp}/{fn}"

            if fn.endswith(".png"):
                with Image.open(experiment_dir / fn) as img:
                    metadata = img.info
                properties[fn_line] = metadata
                filename_list.append(fn_line)
            elif fn.endswith(".pdf"):
                with PdfReader(experiment_dir / fn) as pdf_reader:
                    metadata = pdf_reader.metadata
                properties[fn_line] = metadata
                filename_list.append(fn_line)
            elif fn.endswith(".svg"):
                root = ET.parse(experiment_dir / fn).getroot()
                desc = root.find("{http://www.w3.org/2000/svg}desc")
                metadata = {}
                if desc is not None and desc.text:
                    for line in desc.text.strip().split("\n"):
                        if ": " in line:
                            key, value = line.split(": ", 1)
                            metadata[key] = value
                properties[fn_line] = metadata
                filename_list.append(fn_line)

        grouping = {}
        for key, val in diagnostics.items():
            if isinstance(val, list):
                grouping[key] = [fn for fn in filename_list for v in val if fnmatch(fn, f"*{v}*")]
            else:
                grouping[key] = [fn for fn in filename_list if fnmatch(fn, f"*{val}*")]

        # Add other diagnostics to the grouping
        diagnostics_vals = []
        for key, val in diagnostics.items():
            if isinstance(val, str):
                diagnostics_vals.append(val)
            elif isinstance(val, list):
                diagnostics_vals.extend(val)

        other_group = [fn for fn in filename_list if all(not fnmatch(fn, f"*{val}*") for val in diagnostics_vals)]
        if other_group:
            grouping["Other diagnostics"] = other_group

        content["files"] = {}
        content["diagnostics"] = []
        for key, val in grouping.items():
            if val:
                content["diagnostics"].append(key)
            for v in val:
                content["files"][v] = {"grouping": str(key)}
                prop = properties.get(v, {})
                if prop:
                    for kk, vv in prop.items():
                        keystr = str(kk).lstrip("/").lower()
                        content["files"][v][keystr] = str(vv)

        with (experiment_dir / "content.yaml").open("w") as file:
            yaml.dump(content, file, default_style='"')
        logger.debug(f"Successfully wrote {path}/content.yaml")

        # Convert content to JSON
        content_json = json.dumps(content, indent=4)

        # Write content JSON to file
        with (experiment_dir / "content.json").open("w") as file:
            file.write(content_json)
        logger.debug(f"Successfully wrote {path}/content.json")

    else:
        logger.info(f"Content files for {path} already exist. Skipping. Use --force to overwrite.")


def main(force=False, experiment=None, configfile="config.yaml", ensemble=True, loglevel="INFO", format="png", root="."):
    """
    Main function to create content.yaml and content.json files for each experiment in the content/png directory.

    Args:
        force (bool): Create content.yaml and content.json even if they exist already
        ensemble (bool): If True, assumes a new structure with 4 levels (catalog/model/experiment/realization).
        experiment (str): Specific experiment for which to create content (in format "$catalog/$model/$experiment")
        configfile (str): Alternate confg file path (default "config.yaml" - used by aqua-web)
        loglevel (str): The logging level to use (e.g., 'INFO', 'DEBUG').
        format (str): Image format subdirectory to process.
        root (str or Path): Root of the aqua-web checkout. Defaults to the current directory.
    """

    # Configure logging
    logger.setLevel(loglevel.upper())
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s :: %(name)s :: %(levelname)s -> %(message)s")
    handler.setFormatter(formatter)
    if not logger.handlers:
        logger.addHandler(handler)

    logger.info(f"Starting content generation with log level {loglevel}")
    try:
        with open(configfile, "r") as file:
            config = yaml.safe_load(file)
    except FileNotFoundError as exc:
        raise FileNotFoundError(f"Configuration file not found: {configfile}") from exc

    content_dir = Path(root) / "content" / format
    logger.info("Using content directory %s", content_dir)

    diagnostics = config.get("diagnostics", {})
    if not diagnostics:
        logger.warning("No 'diagnostics' section found in config file.")
    config_experiments = config.get("experiments", {})

    if experiment:
        logger.info(f"Processing single experiment: {experiment}")
        parts = experiment.split("/")

        if len(parts) == 3:
            catalog, model, exp = parts
            realization = None
        elif len(parts) == 4:
            catalog, model, exp, realization = parts
        else:
            raise ValueError("Experiment must be in the format $catalog/$model/$experiment[/realization]")

        make_content(catalog, model, exp, realization, diagnostics, config_experiments, force, content_dir)
    else:  # run through all subdirs
        logger.info("Processing all subdirectories...")
        experiments_file = content_dir.parent / "experiments.yaml"
        if experiments_file.exists():  # Let's start fresh
            logger.info("Removing existing experiments.yaml to start fresh.")
            experiments_file.unlink()

        for catalog in os.listdir(content_dir):
            catalog_path = content_dir / catalog
            if not os.path.isdir(catalog_path):
                continue
            logger.debug(f"Scanning catalog: {catalog}")
            for model in os.listdir(catalog_path):
                model_path = catalog_path / model
                if not os.path.isdir(model_path):
                    continue
                logger.debug(f"Scanning model: {model}")
                for exp in os.listdir(model_path):
                    exp_path = model_path / exp
                    if not os.path.isdir(exp_path):
                        continue
                    logger.debug(f"Scanning experiment: {exp}")

                    if ensemble:  # If new structure, iterate through 4 levels
                        for realization in os.listdir(exp_path):
                            realization_path = exp_path / realization
                            if not os.path.isdir(realization_path):
                                continue
                            logger.debug(f"Processing ensemble member: {catalog}/{model}/{exp}/{realization}")
                            make_content(catalog, model, exp, realization, diagnostics, config_experiments, force, content_dir)
                    else:
                        logger.debug(f"Processing experiment: {catalog}/{model}/{exp}")
                        make_content(catalog, model, exp, None, diagnostics, config_experiments, force, content_dir)


def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(
        description="Create content.yaml and content.json files for each experiment in the content/<format> directory."
    )

    parser.add_argument(
        "-n",
        "--no-ensemble",
        action="store_false",
        dest="ensemble",
        help=(
            "When processing all subdirectories, revert to old structure with 3 levels "
            "(catalog/model/experiment) instead of default 4 levels "
            "(catalog/model/experiment/realization)."
        ),
    )
    parser.add_argument(
        "-f", "--force", action="store_true", help="Create content.yaml and content.json even if they exist already"
    )
    parser.add_argument(
        "-e",
        "--experiment",
        type=str,
        help=(
            "Specific experiment for which to create content in "
            "format $catalog/$model/$experiment/$realization. Realization is optional."
        ),
    )
    parser.add_argument("-c", "--config", type=str, default="config.yaml", help="Alternate config file")
    parser.add_argument(
        "-l",
        "--loglevel",
        type=str,
        default="INFO",
        help="Set the logging level (e.g., DEBUG, INFO, WARNING). Default is INFO.",
    )
    parser.add_argument(
        "--format", type=str, default="png", help="Input data formats to work on (png, pdf or svg). Default: png"
    )

    return parser.parse_args(arguments)


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    force = args.force
    experiment = args.experiment
    config = args.config
    ensemble = args.ensemble
    loglevel = args.loglevel
    format = args.format
    try:
        main(force=force, experiment=experiment, configfile=config, ensemble=ensemble, loglevel=loglevel, format=format)
    except (FileNotFoundError, ValueError) as exc:
        logger.error("Content generation failed: %s", exc)
        raise SystemExit(1) from exc

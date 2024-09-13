#!/usr/bin/env python3
"""
Script to loop on multiple datasets for weight generation using the Reader.

This script initializes the Reader class to use the catalog `config/config.yaml`
for identifying required data and calculating weights based on various parameters.
"""
import os
import sys
import argparse
from aqua import Reader, inspect_catalog
from aqua.logger import log_configure
from aqua.util import load_yaml, get_arg
import subprocess
import time

def parse_arguments(args):
    """
    Parse command line arguments.

    Args:
        args (list): List of arguments passed from the command line.

    Returns:
        Namespace: The parsed arguments as a Namespace object.
    """
    # Initial setup for configuration file parsing
    script_dir = os.path.dirname(os.path.abspath(__file__))
    default_config_file = os.path.join(script_dir, 'weights_config.yml')
    temp_parser = argparse.ArgumentParser(add_help=False)
    temp_parser.add_argument('--config', type=str, default=default_config_file)
    args, remaining_argv = temp_parser.parse_known_args()

    # Loading configuration and setting up the main parser
    config = load_yaml(args.config)
    parser = argparse.ArgumentParser(description='Weights Generator CLI')
    parser.add_argument('--config', type=str, default=args.config)

    parser.add_argument('--catalog', action='store_true', required=False,
                        help='calculate the weights for entire catalog',
                        default=config['catalog'])
    parser.add_argument('-l', '--loglevel', type=str, required=False,
                        help='log level',
                        default=config['loglevel'])
    parser.add_argument('--nproc', type=int, required=False,
                        help='the number of processes to run in parallel',
                        default=config['compute_resources']['nproc'])
    parser.add_argument('-m', '--model', type=str, required=False,
                        help='model name',
                        default=config['data']['models'])
    parser.add_argument('-e', '--exp', type=str, required=False,
                        help='experiment name',
                        default=config['data']['experiments'])
    parser.add_argument('-s', '--source', type=str, required=False,
                        help='source name',
                        default=config['data']['sources'])
    parser.add_argument('-r', '--resolution', type=str, required=False,
                        help='resolution of the grid',
                        default=config['data']['resolutions'])
    parser.add_argument('--zoom_max', type=int, required=False,
                        help='the maximum value of zoom',
                        default=config['data']['zoom_max'])
    parser.add_argument('--rebuild', action='store_true', required=False,
                        help='force rebuilding of area and weight files',
                        default=config['rebuild'])
    parser.add_argument('--resubmit', action='store_true', required=False,
                        help='resubmit a new sbatch job after the current job is completed',
                        default=config['compute_resources']['resubmit'])
    return parser.parse_args()

def validate_config(logger=None, args=None):
    """
    Validate the loaded configuration.

    Args:
        logger (Logger): The logger object for logging messages.
        args (Namespace): The argparse Namespace containing the arguments.

    Raises:
        ValueError: If any configuration validation fails.
    """
    validations = {
        'catalog': (bool, None),
        'rebuild': (bool, None),
        'loglevel': (str, None),
        'model': (list, None),
        'exp': (list, None),
        'source': (list, None),
        'resolution': (list, None),
        'nproc': (int, 1),
        'zoom_max': (int, 1)
    }

    for arg, (expected_type, min_value) in validations.items():
        value = getattr(args, arg, None)
        if not isinstance(value, expected_type) or (min_value is not None and value < min_value):
            message = f"{arg} must be a {expected_type.__name__}"
            if min_value is not None:
                message += f" and no less than {min_value}"
            raise ValueError(message)

def validate_models(logger=None, full_catalog=None, models=None, experiments=None, sources=None, resolutions=None):
    """
    Validate the models and exit if necessary.

    Args:
        logger (Logger): The logger object for logging messages.
        full_catalog (bool): Flag to indicate processing the full catalog.
        models (list): List of model IDs.
        experiments (list): List of experiment IDs.
        sources (list): List of source IDs.
        resolutions (list): List of resolutions.

    Exits:
        1: If the required input parameters are not provided.
    """

    if full_catalog:
        models, experiments, sources = [], [], []
    else:
        models = ensure_list(models)
        experiments = ensure_list(experiments)
        sources = ensure_list(sources)
        resolutions = ensure_list(resolutions)

    if not full_catalog and (not models or not experiments or not sources):
        logger.error("If you do not want to generate weights for the entire catalog, "
                     "you must provide non-empty lists of models, experiments, and sources.")
        sys.exit(1)
    elif full_catalog:
        logger.info("The weights will be generated for the entire catalog.")
    else:
        logger.info("The weights will be generated for the specified models, experiments, and sources.")

def ensure_list(value=None):
    """
    Ensure that the input is a list.

    Args:
        value: A single value or a list.

    Returns:
        list: A list containing the input value(s).
    """
    return [value] if not isinstance(value, list) else value

def calculate_weights(logger=None, model=None, exp=None, source=None, regrid=None, zoom=None, nproc=None, rebuild=None):
    """
    Calculate weights for a specific combination of parameters.

    Uses the Reader class for calculating weights based on provided model, experiment,
    source, and other parameters.

    Args:
        logger (Logger): Logger for logging the process.
        model (str): Model ID.
        exp (str): Experiment ID.
        source (str): Source ID.
        regrid (str): Regridding parameter.
        zoom (int): Zoom level.
        nproc (int): Number of processes.
        rebuild (bool): Flag to rebuild area and weight files.

    Catches:
        Exception: Logs any unexpected error during weight calculation.
    """
    logger.debug(f"The weights are calculating for {model} {exp} {source} {regrid} {zoom}")
    try:
        t_1 = time.time()
        Reader(model=model, exp=exp, source=source, regrid=regrid, zoom=zoom, nproc=nproc, rebuild=rebuild)
        t_2 = time.time()
        if t_2-t_1>10:
            logger.error(f"Took {t_2-t_1} seconds to rebuild")
    except Exception as e:
        logger.error(f"An unexpected error occurred for source {model} {exp} {source} {regrid} {zoom}: {e}")

def generate_weights(logger=None, full_catalog=None, resolutions=None, models=None, experiments=None, sources=None,
                     nproc=None, zoom_max=None, rebuild=None):
    """
    Generate weights based on provided parameters.

    Iterates over different combinations of models, experiments, sources, and other
    parameters to generate weights.

    Args:
        logger (Logger): Logger for logging the process.
        full_catalog (bool): Flag to process the full catalog.
        resolutions (list): List of resolutions.
        models (list): List of model IDs.
        experiments (list): List of experiment IDs.
        sources (list): List of source IDs.
        nproc (int): Number of processes.
        zoom_max (int): Maximum zoom level.
        rebuild (bool): Flag to rebuild area and weight files.
    """
    logger.info("Weight generation is started.")
    
    for reso in resolutions:
        for model in models or inspect_catalog():
            for exp in experiments or inspect_catalog(model=model):
                for source in sources or inspect_catalog(model=model, exp=exp):
                    for zoom in range(zoom_max):
                        calculate_weights(logger=logger, model=model, exp=exp, source=source, regrid=reso, zoom=zoom,
                                          nproc=nproc, rebuild=rebuild)
def resubmit_job(logger=None):
    """
    Resubmits the current job by executing a separate Bash script.

    This function calls a Bash script 'run_weights_submitter.sh' located in
    the same directory as the current script. The Bash script is responsible
    for scheduling a new SLURM job. This function should be called at the
    end of the job's execution if automatic resubmission is desired.

    Uses `subprocess.call` to execute the Bash script. Ensure that the
    Bash script has execute permissions and contains the correct sbatch
    command for resubmission.
    """
    script_dir = os.path.dirname(os.path.abspath(__file__))
    path_to_bash_script = os.path.join(script_dir, 'run_weights_submitter.sh')
    try:
        bash_command = f"bash {path_to_bash_script}"
        subprocess.call(bash_command, shell=True)
        logger.info("The 'run_weights_submitter.sh' script is running.")
    except FileNotFoundError:
        logger.error(f"File not found: {path_to_bash_script}")
    except Exception as e:
        logger.error(f"Error occurred: {e}")

def main():
    """
    Main function to orchestrate the weight generation process.
    """
    args = parse_arguments(sys.argv[1:])
    logger = log_configure(log_name='Weights Generator', log_level=args.loglevel)
    validate_models(logger=logger, full_catalog=args.catalog, models=args.model, experiments=args.exp,
                           sources=args.source, resolutions=args.resolution)
    validate_config(logger=logger, args=args)
    generate_weights(logger=logger, full_catalog=args.catalog, resolutions=args.resolution, models=args.model,
                     experiments=args.exp, sources=args.source, nproc=args.nproc, zoom_max=args.zoom_max, rebuild=args.rebuild)
    if args.resubmit:
        resubmit_job(logger=logger)

if __name__ == "__main__":
    main()
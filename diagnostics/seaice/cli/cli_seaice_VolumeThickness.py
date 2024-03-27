#!/usr/bin/env python3

"""
Sea ice Diagnostic CLI. Strongly Inspired from its SSH equivalent

This script allows users to execute sea ice diagnostics using command-line arguments.
By default, it will read configurations from 'config.yaml' unless specified by the user.
"""

import argparse
import os
import sys

sys.path.insert(0, "../..")

from dask.distributed import Client, LocalCluster

# Imports related to the aqua package, which is installed and available globally.
from aqua.logger import log_configure
from aqua.util import get_arg, load_yaml
from aqua.exceptions import NoDataError

# Local module imports.
from seaice import SeaIceExtent, SeaIceVolume, SeaIceConcentration, SeaIceThickness

# Add the directory containing the `seaice` module to the Python path.
# Since the module is in the parent directory of this script, we calculate the script's directory
# and then move one level up.
# change the current directory to the one of the CLI so that relative path works
abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)

if os.getcwd() != dname:
    os.chdir(dname)
    print(f'Moving from current directory to {dname} to run!')

script_dir = dname


def parse_arguments(args):
    """
    Parse command line arguments.

    :param args: List of command line arguments.
    :return: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='sea ice CLI')

    # Arguments for the CLI.
    parser.add_argument('--config', type=str, default='config.yaml',
                        help=f'yaml configuration file (default: config.yaml)')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument('--all-regions', action='store_true',
                        help='Compute sea ice extent for all regions')
    parser.add_argument('--loglevel', '-l', type=str, default='WARNING',
                        help='Logging level (default: WARNING)')

    # These arguments override the configuration file if provided.
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str, help='Output directory')
    parser.add_argument('--regrid', type=str, help='Target regrid resolution')

    return parser.parse_args(args)


if __name__ == '__main__':
    # Add the directory containing the `seaice` module to the Python path.
    # Since the module is in the parent directory of this script, we calculate the script's directory
    # and then move one level up.
    # change the current directory to the one of the CLI so that relative path works
    # Parse the provided command line arguments.

    args = parse_arguments(sys.argv[1:])

    # Configure the logger.
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name="SeaIce CLI", log_level=loglevel)

    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)

    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    sys.path.insert(0, "../..")

    # Local module imports.
    from seaice import SeaIceExtent

    logger.info("Running sea ice diagnostic...")

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Outputdir
    outputdir = get_arg(args, 'outputdir', None)
    logger.debug(f"Output directory: {outputdir}")

    # Read configuration file.
    # We first load a config.yaml file from the current directory,
    # then if present, we override the first model with the CLI arguments.
    logger.info('Reading configuration yaml file...')
    config = load_yaml(args.config)
    logger.debug(f"Configuration file: {config}")

    # Override configurations with CLI arguments if provided.
    config['models'][0]['model'] = get_arg(args, 'model',
                                           config['models'][0]['model'])
    config['models'][0]['exp'] = get_arg(args, 'exp',
                                         config['models'][0]['exp'])
    config['models'][0]['source'] = get_arg(args, 'source',
                                            config['models'][0]['source'])
    config['models'][0]['regrid'] = get_arg(args, 'regrid',
                                            config['models'][0]['regrid'])
    config['output_directory'] = get_arg(args, 'outputdir',
                                         config['output_directory'])

    # If the user wants to compute sea ice extent for all regions, we override the
    # configuration file.
    if args.all_regions:
        config['regions'] = None

    logger.debug(f"Final configuration: {config}")

    outputdir = config['output_directory']

#    # Initialize the object SeaIceExtent
#    analyzer = SeaIceExtent(config=config, outputdir=outputdir,
#                            loglevel=loglevel)

#    # Execute the analyzer.
#    try:
#        analyzer.run()
#    except NoDataError as e:
#        logger.debug(f"Error: {e}")
#        logger.error("No data found for the given configuration. Exiting...")
#        sys.exit(0)
#    except Exception as e:
#        logger.error(f"An error occurred while running the analyzer: {e}")
#        logger.warning("Please report this error to the developers. Exiting...")
#        sys.exit(0)
#
#    logger.info("sea ice diagnostic Extent terminated!")


    # Initialize the object SeaIceVolume
    # analyzer = SeaIceVolume(config=config, outputdir=outputdir,
    #                         loglevel=loglevel)

    # # Execute the analyzer.
    # try:
    #     analyzer.run()
    # except NoDataError as e:
    #     logger.debug(f"Error: {e}")
    #     logger.error("No data found for the given configuration. Exiting...")
    #     sys.exit(0)
    # except Exception as e:
    #     logger.error(f"An error occurred while running the analyzer: {e}")
    #     logger.warning("Please report this error to the developers. Exiting...")
    #     sys.exit(0)

    # logger.info("sea ice diagnostic Volume terminated!")



# Initialize the object SeaIceConcentration
analyzer = SeaIceConcentration(config=config, outputdir=outputdir,
                        loglevel=loglevel)

# Execute the analyzer.
try:
    analyzer.run()
except NoDataError as e:
    logger.debug(f"Error: {e}")
    logger.error("No data found for the given configuration. Exiting...")
    sys.exit(0)
except Exception as e:
    logger.error(f"An error occurred while running the analyzer: {e}")
    logger.warning("Please report this error to the developers. Exiting...")
    sys.exit(0)

logger.info("sea ice diagnostic Concentration terminated!")

 
## Initialize the object SeaIceThickness
#analyzer = SeaIceThickness(config="config_VolumeThickness.yaml", outputdir=outputdir,
#                         loglevel=loglevel)
#
## Execute the analyzer.
#try:
#    analyzer.run()
#except NoDataError as e:
#     logger.debug(f"Error: {e}")
#     logger.error("No data found for the given configuration. Exiting...")
#     sys.exit(0)
#except Exception as e:
#     logger.error(f"An error occurred while running the analyzer: {e}")
#     logger.warning("Please report this error to the developers. Exiting...")
#     sys.exit(0)

#logger.info("sea ice diagnostic Thickness terminated!")   

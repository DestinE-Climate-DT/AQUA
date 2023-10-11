#!/usr/bin/env python3

"""
Sea ice Diagnostic CLI. Strongly Inspired from SSH equivalent

This script allows users to execute sea ice diagnostics using command-line arguments.
By default, it will read configurations from 'config.yml' unless specified by the user.
"""

import argparse
import os
import sys

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
sys.path.insert(0, "../..")
# script_dir = os.path.dirname(os.path.abspath(__file__))
# seaice_module_path = os.path.join(script_dir, "../../")
# sys.path.insert(0, seaice_module_path)

# Local module imports.
from seaice import SeaIceExtent

# Imports related to the aqua package, which is installed and available globally.
from aqua import Reader
from aqua.logger import log_configure
from aqua.util import get_arg


def parse_arguments(args):
    """
    Parse command line arguments.

    :param args: List of command line arguments.
    :return: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='sea ice CLI')

    # Define the default path for the configuration file.
    default_config_path = os.path.join(script_dir, 'config.yml')
     
    # Define the default path for the file used for selection.
    default_region_path = os.path.join(script_dir, 'regions_selection.yml')
    
    # Arguments for the CLI.
    parser.add_argument('--config', type=str, default=default_config_path,
                        help=f'yaml configuration file (default: {default_config_path})')
    parser.add_argument('--loglevel', '-l', type=str, default='WARNING',
                        help='Logging level (default: WARNING)')

    # These arguments override the configuration file if provided.
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str, help='Output directory')
    parser.add_argument('--regions'  , type = str, default=default_region_path,
                        help='Region selection file (default: all regions)')

    return parser.parse_args(args)


if __name__ == '__main__':
    print("Running sea ice diagnostic...")

    # Parse the provided command line arguments.
    args = parse_arguments(sys.argv[1:])

    # Configure the logger.
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name="SeaIce CLI", log_level=loglevel)

    # Outputdir
    outputdir = get_arg(args, 'outputdir', None)
    logger.debug(f"Output directory: {outputdir}")

    # Read configuration file.
    logger.warning('Reading configuration yaml file...')
    analyzer = SeaIceExtent(args.config, loglevel=loglevel, outputdir=outputdir)

    # Override configurations with CLI arguments if provided.
    analyzer.config['models'][0]['name'] = get_arg(args, 'model', analyzer.config['models'][0]['name'])
    analyzer.config['models'][0]['experiment'] = get_arg(args, 'exp', analyzer.config['models'][0]['experiment'])
    analyzer.config['models'][0]['source'] = get_arg(args, 'source', analyzer.config['models'][0]['source'])
    analyzer.config['output_directory'] = get_arg(args, 'outputdir', analyzer.config['output_directory'])

    logger.debug(f"Configuration: {analyzer.config}")

    # Execute the analyzer.
    analyzer.run()
    analyzer.computeExtent()
    analyzer.createNetCDF()

    print("sea ice diagnostic completed!")

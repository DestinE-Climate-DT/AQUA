#!/usr/bin/env python3

"""
SSH Variability Diagnostic CLI.

This script allows users to execute SSH variability diagnostics using command-line arguments.
By default, it will read configurations from 'config.yaml' unless specified by the user.
"""

import argparse
import os
import sys
from datetime import datetime

# Add the directory containing the `ssh` module to the Python path.
# Since the module is in the parent directory of this script, we calculate the script's directory
# and then move one level up.
script_dir = os.path.dirname(os.path.abspath(__file__))
ssh_module_path = os.path.join(script_dir, "../../")
sys.path.insert(0, ssh_module_path)

# Local module imports.
from ssh import sshVariability

# Imports related to the aqua package, which is installed and available globally.
from aqua.util import get_arg


def valid_date(s):
    try:
        datetime.strptime(s, "%Y-%m-%d")
        return s
    except ValueError:
        msg = "Not a valid date: '{0}'.".format(s)
        raise argparse.ArgumentTypeError(msg)


def parse_arguments(args):
    """
    Parse command line arguments.

    :param args: List of command line arguments.
    :return: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description='SSH variability CLI')

    # Define the default path for the configuration file.
    default_config_path = os.path.join(script_dir, 'config.yaml')

    # Arguments for the CLI.
    parser.add_argument('--config', type=str, default=default_config_path,
                        help=f'yaml configuration file (default: {default_config_path})')
    parser.add_argument('-n', '--nworkers', type=int,
                    help='number of dask distributed workers')
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str, help='Output directory')
    parser.add_argument('--modeltime', default=None, nargs=2, type=valid_date,
                        help='Model time span in the format: "YYYY-MM-DD", "YYYY-MM-DD"')
    parser.add_argument('--obstime', default=None, nargs=2, type=valid_date,
                    help='Observation time span in the format: "YYYY-MM-DD", "YYYY-MM-DD"')
    parser.add_argument('--loglevel', type=str, choices=['DEBUG', 'INFO', 'WARNING'],
                        help='Log level for the SSH variability diagnostic')
    parser.add_argument('--regrid', type=str, choices=['r005', 'r010', 'r025', 'r050', 'r100'],
                    help='Regrid option (choose from: r005, r010, r025, r050, r100)')

    return parser.parse_args(args)


if __name__ == '__main__':
    print("Running SSH variability diagnostic...")

    # Parse the provided command line arguments.
    args = parse_arguments(sys.argv[1:])

    # Read configuration file.
    print('Reading configuration yaml file...')
    analyzer = sshVariability(args.config)

    # Override configurations with CLI arguments if provided.
    analyzer.config['models'][0]['name'] = get_arg(args, 'model', analyzer.config['models'][0]['name'])
    analyzer.config['models'][0]['experiment'] = get_arg(args, 'exp', analyzer.config['models'][0]['experiment'])
    analyzer.config['models'][0]['source'] = get_arg(args, 'source', analyzer.config['models'][0]['source'])
    analyzer.config['output_directory'] = get_arg(args, 'outputdir', analyzer.config['output_directory'])

    #if "timespan" in analyzer.config['models'][0] and args.modeltime is not None:
    if "timespan" in analyzer.config['models'][0] and args.modeltime is not None:
        analyzer.config['models'][0]['timespan'] = get_arg(args, 'modeltime', analyzer.config['models'][0]['timespan'])

    if args.obstime is not None:
        timespan = {'start': args.obstime[0], 'end': args.obstime[1]}
        analyzer.config['timespan'] = timespan

    if args.modeltime is not None:
        timespan = {'start': args.modeltime[0], 'end': args.modeltime[1]}
        analyzer.config['timespan'] = timespan

    if args.loglevel is not None:
        analyzer.config['log_level'] = args.loglevel

    if "regrid" in analyzer.config['models'][0] and args.regrid is not None:
        analyzer.config['models'][0]['regrid'] = args.regrid
        
    # Execute the analyzer.
    analyzer.run(nworkers=args.nworkers,args=args)
    print("SSH variability diagnostic completed!")

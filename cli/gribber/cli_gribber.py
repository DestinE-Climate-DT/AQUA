#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line tool to create .json files and
update the catalog yaml files with new entry
from the desired .grib file.
"""

import argparse
import sys
from aqua.gribber import Gribber
from aqua.util import load_yaml, get_arg


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA gribber')
    parser.add_argument('-c', '--config', type=str, required=False,
                        help='yaml file with exp information and directories')
    parser.add_argument('-n', '--nprocs', type=int, required=False,
                        help='Number of processors')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('-o', '--overwrite', action='store_true',
                        required=False,
                        help='Overwrite JSON file and indices if they exist')

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')

    file = get_arg(args, 'config', 'gribber_config.yaml')

    config = load_yaml(file)

    model = config['id']['model']
    exp = config['id']['exp']
    source = config['id']['source']
    nprocs = get_arg(args, 'nprocs', 1)
    overwrite = get_arg(args, 'overwrite', False)

    # Create Gribber object
    gribber = Gribber(model=model, exp=exp, source=source, nprocs=nprocs,
                      dir=config['dir'],
                      loglevel=loglevel, overwrite=overwrite)

    # Create .catalog entry
    gribber.create_entry(loglevel)

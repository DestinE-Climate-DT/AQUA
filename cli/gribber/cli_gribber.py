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
    parser.add_argument('-s', '--search', action='store_true',
                        required=False,
                        help='Search for generic grib files names')

    return parser.parse_args(args)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')

    file = get_arg(args, 'config', 'gribber_config.yaml')

    config = load_yaml(file)

    model = config['id']['model']
    exp = config['id']['exp']
    source = config['id']['source']
    description = config['id']['description']
    nprocs = get_arg(args, 'nprocs', 1)
    overwrite = get_arg(args, 'overwrite', False)
    search = get_arg(args, 'search', False)

    # Create Gribber object
    gribber = Gribber(model=model, exp=exp, source=source, nprocs=nprocs,
                      dirdict=config['dir'], description=description,
                      loglevel=loglevel, overwrite=overwrite,
                      search=search)

    # Create .catalog entry
    gribber.create_entry(loglevel)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import argparse
from tropical_rainfall import __path__ as pypath

def parse_arguments():
    """Parse arguments for Tropical Rainfall console"""

    parser = argparse.ArgumentParser(prog='tropical_rainfall', description='Tropical Rainfall command line tool')
    subparsers = parser.add_subparsers(dest='command', help='Available Tropical Rainfall commands')

    parser.add_argument('--path', action='store_true',
                        help="Show Tropical Rainfall installation path and exit")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO log level')
    parser.add_argument('-vv', '--very_verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG log level')

    add_config_parser = subparsers.add_parser('add_config', help='Add a configuration file to Tropical Rainfall')
    add_config_parser.add_argument('config_file_path', nargs='?', default=None, help='Path to the configuration file')

    return parser



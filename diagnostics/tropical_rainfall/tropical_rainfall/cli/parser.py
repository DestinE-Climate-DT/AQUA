#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Tropical Rainfall command line parser
'''

import argparse
from tropical_rainfall import __version__ as version
from tropical_rainfall import __path__ as pypath

def parse_arguments():
    """Parse arguments for Tropical Rainfall console"""

    parser = argparse.ArgumentParser(prog='tropical_rainfall', description='Tropical Rainfall command line tool')
    subparsers = parser.add_subparsers(dest='command', help='Available Tropical Rainfall commands')

    # Parser for the tropical_rainfall main command
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s v{version}', help="show Tropical Rainfall version number and exit.")
    parser.add_argument('--path', action='version', version=f'{pypath[0]}',
                        help="show Tropical Rainfall installation path and exit")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO loglevel')
    parser.add_argument('-vv', '--very-verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG loglevel')

    # List of the subparsers with actions
    init_parser = subparsers.add_parser("init", description='Initialize Tropical Rainfall configuration')
    add_parser = subparsers.add_parser("add", description='Add a configuration')
    update_parser = subparsers.add_parser("update", description='Update a configuration')
    remove_parser = subparsers.add_parser("remove", description='Remove a configuration')
    set_parser = subparsers.add_parser("set", description="Set a configuration as the default")
    list_parser = subparsers.add_parser("list", description="List the currently installed configurations")

    # subparser with no arguments
    subparsers.add_parser("uninstall", description="Remove the current Tropical Rainfall installation")

    # extra parsers arguments
    init_parser.add_argument('machine', nargs='?', metavar="MACHINE_NAME", default=None,
                                help="Machine on which to initialize Tropical Rainfall")
    init_parser.add_argument('-p', '--path', type=str, metavar="TROPICAL_RAINFALL_TARGET_PATH",
                                help='Path where to initialize Tropical Rainfall. Default is $HOME/.tropical_rainfall')
    init_parser.add_argument('-e', '--editable', type=str, metavar="TROPICAL_RAINFALL_SOURCE_PATH",
                                help='Initialize Tropical Rainfall in editable mode from the original source')

    add_parser.add_argument("config", metavar="CONFIG_NAME",
                            help="Configuration to be added")
    add_parser.add_argument('-e', '--editable', metavar="CONFIG_SOURCE_PATH", type=str,
                            help='Add a configuration in editable mode from the original source')

    remove_parser.add_argument("config", metavar="CONFIG_NAME",
                               help="Configuration to be removed")

    set_parser.add_argument("config", metavar="CONFIG_NAME", help="Configuration to be used in Tropical Rainfall")

    update_parser.add_argument("config", metavar="CONFIG_NAME", help="Configuration to be updated")

    list_parser.add_argument("-a", "--all", action="store_true",
                             help="Print also all the installed configurations")

    # create a dictionary to simplify the call
    parser_dict = {
        'main': parser
    }

    return parser_dict

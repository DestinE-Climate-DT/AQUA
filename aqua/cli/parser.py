#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA command line parser
'''


import argparse
from aqua import __version__ as version


def parse_arguments():
    """Parse arguments for AQUA console"""

    parser = argparse.ArgumentParser(prog='aqua', description='AQUA command line tool')
    subparsers = parser.add_subparsers(dest='command', help='Available AQUA commands')

    # Parser for the aqua main command
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s {version}', help="show AQUA version number and exit.")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO loglevel')
    parser.add_argument('-vv', '--very-verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG loglevel')

    # List of the subparsers with actions
    # Corresponding to the different aqua commands available (see command map)
    install_parser = subparsers.add_parser("install", description='Install AQUA configuration files')
    catalog_add_parser = subparsers.add_parser("add", description='Add a catalog in the current AQUA installation')
    catalog_update_parser = subparsers.add_parser("update", description='Update a catalog in the current AQUA installation')
    catalog_remove_parser = subparsers.add_parser("remove", description='Remove a catalog in the current AQUA installation')
    set_parser = subparsers.add_parser("set", description="Set an installed catalog as the predefined in config-aqua.yaml")
    list_parser = subparsers.add_parser("list", description="List the currently installed AQUA catalogs")

    # subparser with no arguments
    subparsers.add_parser("uninstall", description="Remove the current AQUA installation")

    # subparsers for grids and fixes
    parser_grids = file_subparser(subparsers, 'grids')
    parser_fixes = file_subparser(subparsers, 'fixes')

    # extra parsers arguments
    install_parser.add_argument('-p', '--path', type=str,
                                help='Path where to install AQUA. Default is $HOME/.aqua')
    install_parser.add_argument('-g', '--grids', type=str,
                                help='Path where to be usef for AQUA grids (NOT WORKING FOR NOW)')
    install_parser.add_argument('-e', '--editable', type=str,
                                help='Install AQUA in editable mode from the original source')

    catalog_add_parser.add_argument("catalog", metavar="CATALOG",
                                    help="Catalog to be installed")
    catalog_add_parser.add_argument('-e', '--editable', type=str,
                                    help='Install a catalog in editable mode from the original source: provide the Path')
    catalog_add_parser.add_argument("-g", "--github", action="store_true",
                             help="Retrieve the AQUA catalog from Github repository")
    
    catalog_update_parser.add_argument("-g", "--github", action="store_true",
                             help="Retrieve the AQUA catalog from Github repository")


    catalog_remove_parser.add_argument("catalog", metavar="CATALOG",
                                       help="Catalog to be removed")

    set_parser.add_argument("catalog", metavar="CATALOG", help="Catalog to be used in AQUA")

    catalog_update_parser.add_argument("catalog", metavar="CATALOG", help="Catalog to be updated")

    list_parser.add_argument("-a", "--all", action="store_true",
                             help="Print also all the installed fixes, grids and data_models")

    # create a dictionary to simplify the call
    parser_dict = {
        'main': parser,
        'fixes': parser_fixes,
        'grids': parser_grids
    }

    return parser_dict


def file_subparser(main_parser, name):
    """Compact subparsers for file handling - fixes and grids"""

    # subparsers for fixes
    parser = main_parser.add_parser(name, help=f'{name} related commands')
    subparsers = parser.add_subparsers(dest='nested_command')

    parser_add = subparsers.add_parser('add', help=f'Add a {name} file in the current AQUA installation')
    parser_add.add_argument('file', help=f'The {name} yaml file to add')
    parser_add.add_argument("-e", "--editable", action="store_true",
                                  help=f"Add a {name} file in editable mode from the original path")
    parser_remove = subparsers.add_parser('remove', help=f'Remove a {name} file')
    parser_remove.add_argument('file', help=f'The {name} file to remove')

    return parser

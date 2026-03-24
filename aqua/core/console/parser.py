#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA command line parser
'''
import argparse
from importlib import resources as pypath
from aqua import __version__ as version
from aqua.core.console.analysis import analysis_parser
from aqua.core.console.drop import drop_parser
from aqua.core.console.catgen import catgen_parser
from aqua.core.console.builder import builder_parser


def parse_arguments():
    """Parse arguments for AQUA console"""

    parser = argparse.ArgumentParser(prog='aqua', description='AQUA command line tool')
    subparsers = parser.add_subparsers(dest='command', help='Available AQUA commands')

    # Parser for the aqua main command
    parser.add_argument('--version', action='version',
                        version=f'%(prog)s v{version}', help="show AQUA version number and exit.")
    parser.add_argument('--path', action='version', version=f'{pypath.files("aqua")}',
                        help="show AQUA installation path and exit")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO loglevel')
    parser.add_argument('-vv', '--very-verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG loglevel')

    # List of the subparsers with actions
    # Corresponding to the different aqua commands available (see command map)
    install_parser = subparsers.add_parser("install", description='Install AQUA configuration files')
    catalog_add_parser = subparsers.add_parser("add", description='Add a catalog in the current AQUA installation')
    catalog_update_parser = subparsers.add_parser("update", description='Update the current AQUA installation')
    catalog_remove_parser = subparsers.add_parser("remove", description='Remove a catalog in the current AQUA installation')
    set_parser = subparsers.add_parser("set", description="Set an installed catalog as the predefined in config-aqua.yaml")
    list_parser = subparsers.add_parser("list", description="List the currently installed AQUA catalogs")
    avail_parser = subparsers.add_parser("avail", description='List the ClimateDT available catalogs on GitHub')

    # subparser for other AQUA commands as they are importing the parser from their code
    analysis_subparser = subparsers.add_parser("analysis", description="Run AQUA diagnostics")
    analysis_subparser = analysis_parser(parser=analysis_subparser)

    drop_subparser = subparsers.add_parser("drop", description="Data Reduction OPerator")
    drop_subparser = drop_parser(parser=drop_subparser)
 
    catgen_subparser = subparsers.add_parser("catgen", description="FDB catalog generator")
    catgen_subparser = catgen_parser(parser=catgen_subparser)

    # subparser with no arguments
    subparsers.add_parser("uninstall", description="Remove the current AQUA installation")

    # subparsers for grids and fixes
    parser_grids = file_subparser(subparsers, 'grids')
    parser_fixes = file_subparser(subparsers, 'fixes')

    # extra parsers arguments
    install_parser.add_argument('machine', metavar="MACHINE_NAME",
                                help="Machine on which install AQUA")
    install_parser.add_argument('-p', '--path', type=str, metavar="AQUA_TARGET_PATH",
                                help='Path where to install AQUA. Default is $HOME/.aqua')
    install_parser.add_argument('-c', '--core', nargs='?', const='standard', type=str, metavar="AQUA_CORE_PATH",
                                help='Install AQUA core. Without path: standard installation of core only. '
                                     'With path: editable installation from that path')
    install_parser.add_argument('-d', '--diagnostics', nargs='?', const='standard', type=str, metavar="AQUA_DIAG_PATH",
                                help='Install AQUA diagnostics. Without path: standard installation of diagnostics only. '
                                     'With path: editable installation from that path')
    

    catalog_add_parser.add_argument("catalog", metavar="CATALOG_NAME",
                                    help="Catalog to be installed")
    catalog_add_parser.add_argument('-e', '--editable', type=str,
                                    help='Install a catalog in editable mode from the original source: provide the Path')
    catalog_add_parser.add_argument('-r', '--repository', type=str,
                                    help='Install a catalog from a specific repository: provide the user/repo string')
    
    avail_parser.add_argument('-r', '--repository', type=str,
                              help='Explore a specific repository: provide the user/repo string')

    catalog_remove_parser.add_argument("catalog", metavar="CATALOG_NAME",
                                       help="Catalog to be removed")

    set_parser.add_argument("catalog", metavar="CATALOG_NAME", help="Catalog to be used in AQUA")

    catalog_update_parser.add_argument('-c', '--catalog', type=str,
                                       help='Update a catalog')
    #catalog_update_parser.add_argument("-a", "--all", action="store_true",
    #                         help="Print also all the installed fixes, grids and data_models")

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

    # subparsers for fixe and grids
    parser = main_parser.add_parser(name, help=f'{name} related commands')
    subparsers = parser.add_subparsers(dest='nested_command')

    parser_add = subparsers.add_parser('add', help=f'Add a {name} file in the current AQUA installation')
    parser_add.add_argument('file', help=f'The {name} yaml file to add')
    parser_add.add_argument("-e", "--editable", action="store_true",
                                  help=f"Add a {name} file in editable mode from the original path")
    parser_remove = subparsers.add_parser('remove', help=f'Remove a {name} file')
    parser_remove.add_argument('file', help=f'The {name} file to remove')

    # We have for the grids the possibility to set a default path to overwrite the individual catalog one
    # This will create a block in the config-aqua.yaml file for grids, areas and weights.
    if name == 'grids':
        parser_set = subparsers.add_parser('set', help=f'Set a {name} path as the default in config-aqua.yaml')
        parser_set.add_argument('path', help=f'The {name} path to set as default')
        parser_build = subparsers.add_parser('build', help=f'Build {name} grids from data sources')
        parser_build = builder_parser(parser_build)

    return parser

#!/usr/bin/env python3

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

    parser.add_argument('--version', action='version',
                        version=f'%(prog)s v{version}', help="show Tropical Rainfall version number and exit.")
    parser.add_argument('--path', action='version', version=f'{pypath[0]}',
                        help="show Tropical Rainfall installation path and exit")
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='Increase verbosity of the output to INFO loglevel')
    parser.add_argument('-vv', '--very_verbose', action='store_true',
                        help='Increase verbosity of the output to DEBUG loglevel')

    use_config_parser = subparsers.add_parser("use_config", description='Use a new configuration file for Tropical Rainfall')
    use_config_parser.add_argument('config_file_path', metavar="CONFIG_FILE_PATH", type=str,
                                   help="Path to the configuration file")

    parser_dict = {
        'main': parser
    }

    return parser_dict

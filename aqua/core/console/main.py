#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA command line main functions
'''

import os
from importlib import resources as pypath

from aqua.core.logger import log_configure

from .parser import parse_arguments
from .analysis import analysis_execute
from .builder import builder_execute
from .drop import drop_execute
from .catgen import catgen_execute
from .install import InstallMixin
from .catalog import CatalogMixin
from .files import FilesMixin

# this are used to check existence of aqua.diagnostics
from .install import DIAGNOSTIC_CONFIG_DIRECTORIES, DIAGNOSTIC_TEMPLATE_DIRECTORIES



class AquaConsole(InstallMixin, CatalogMixin, FilesMixin):
    """Class for AquaConsole, the AQUA command line interface for
    installation, catalog, grids and fixes editing"""

    def __init__(self):
        """The main AQUA command line interface"""

        # NOTE: self.corepath points to $AQUA/aqua/core/config folder
        self.corepath = os.path.join(pypath.files('aqua.core'), 'config')
        if DIAGNOSTIC_CONFIG_DIRECTORIES and DIAGNOSTIC_TEMPLATE_DIRECTORIES:
            self.diagpath = os.path.join(pypath.files('aqua.diagnostics'), 'config')
        else:
            self.diagpath = None
        self.configpath = None
        self.templatepath = None
        self.configfile = 'config-aqua.yaml'
        self.grids = None
        self.logger = None
        self.loglevel = 'WARNING'

        self.command_map = {
            'install': self.install,
            'add': self.add,
            'remove': self.remove,
            'set': self.set,
            'uninstall': self.uninstall,
            'avail': self.avail,
            'list': self.list,
            'update': self.update,
            'fixes': {
                'add': self.fixes_add,
                'remove': self.remove_file
            },
            'grids': {
                'add': self.grids_add,
                'remove': self.remove_file,
                'set': self.grids_set,
                'build': self.grids_build
            },
            'analysis': self.analysis,
            'drop': self.drop,
            'catgen': self.catgen
        }

    def execute(self):
        """Parse AQUA class and run the required command"""

        parser_dict = parse_arguments()
        parser = parser_dict['main']
        args = parser.parse_args()

        # Set the log level
        if args.very_verbose or (args.verbose and args.very_verbose):
            self.loglevel = 'DEBUG'
        elif args.verbose:
            self.loglevel = 'INFO'

        self.logger = log_configure(self.loglevel, 'AQUA')

        command = args.command
        method = self.command_map.get(command, parser.print_help)

        if command not in self.command_map:
            parser.print_help()
        else:
            # nested map
            if isinstance(self.command_map[command], dict):
                if args.nested_command:
                    self.command_map[command][args.nested_command](args)
                else:
                    parser_dict[command].print_help()
            # default
            else:
                method(args)

    def analysis(self, args):
        """
        Run the AQUA analysis

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print('Running the AQUA analysis')
        analysis_execute(args)

    def drop(self, args):
        """
        Run the Data Reduction OPerator

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print('Running the Data Reduction OPerator')
        drop_execute(args)

    def catgen(self, args):
        """
        Run the FDB catalog generator

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print("Running the catalog generator")
        catgen_execute(args)

    def grids_build(self, args):
        """Build grids from data sources

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        print('Running AQUA grids builder')
        builder_execute(args)


def main():
    """AQUA main installation tool"""
    aquacli = AquaConsole()
    aquacli.execute()

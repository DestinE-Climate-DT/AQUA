#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Tropical Rainfall command line main functions
'''

import os
import shutil
import sys
from aqua.util import load_yaml, dump_yaml
from aqua.logger import log_configure
from aqua.util import ConfigPath
from tropical_rainfall.cli.parser import parse_arguments
from tropical_rainfall import __path__ as pypath  # Add this line

# folder used for reading/storing configs
config_folder = 'configs'

class TropicalRainfallConsole():
    """Class for TropicalRainfallConsole, the Tropical Rainfall command line interface for
    initialization and configuration management"""

    def __init__(self):
        """The main Tropical Rainfall command line interface"""

        self.pypath = pypath[0]
        self.configpath = None
        self.configfile = 'config-tropical-rainfall.yml'
        self.logger = None

        self.command_map = {
            'init': self.init,
            'add': self.add,
            'remove': self.remove,
            'set': self.set,
            'uninstall': self.uninstall,
            'list': self.list,
            'update': self.update,
            # Add other commands if necessary
        }

    def execute(self):
        """Parse Tropical Rainfall class and run the required command"""

        parser_dict = parse_arguments()
        args = parser_dict['main'].parse_args(sys.argv[1:])

        # Set the log level
        if args.very_verbose or (args.verbose and args.very_verbose):
            loglevel = 'DEBUG'
        elif args.verbose:
            loglevel = 'INFO'
        else:
            loglevel = 'WARNING'
        self.logger = log_configure(loglevel, 'Tropical Rainfall')

        command = args.command
        method = self.command_map.get(command, parser_dict['main'].print_help)
        if command not in self.command_map:
            parser_dict['main'].print_help()
        else:
            method(args)

    def init(self, args):
        """Initialize Tropical Rainfall configuration

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Initializing Tropical Rainfall')

        if args.path is None:
            self._config_home()
        else:
            self._config_path(args.path)

    def _config_home(self):
        """Configure the Tropical Rainfall installation folder, by default inside $HOME"""

        if 'HOME' in os.environ:
            path = os.path.join(os.environ['HOME'], '.tropical_rainfall')
            self.configpath = path
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
            else:
                self.logger.warning('Tropical Rainfall already installed in %s', path)
                check = query_yes_no(f"Do you want to overwrite Tropical Rainfall installation in {path}. "
                                     "You will lose all configurations installed.", "no")
                if not check:
                    sys.exit()
                else:
                    self.logger.warning('Removing the content of %s', path)
                    shutil.rmtree(path)
                    os.makedirs(path, exist_ok=True)
        else:
            self.logger.error('$HOME not found. Please specify a path where to install Tropical Rainfall and define TROPICAL_RAINFALL_CONFIG as environment variable')
            sys.exit(1)

    def _config_path(self, path):
        """Define the Tropical Rainfall installation folder when a path is specified

        Args:
            path (str): the path where to install Tropical Rainfall
        """
        self.configpath = path
        if not os.path.exists(path):
            os.makedirs(path, exist_ok=True)
        else:
            if not os.path.isdir(path):
                self.logger.error("Path chosen is not a directory")
                sys.exit(1)

        check = query_yes_no(f"Do you want to create a link in the $HOME/.tropical_rainfall to {path}", "yes")
        if check:
            if 'HOME' in os.environ:
                link = os.path.join(os.environ['HOME'], '.tropical_rainfall')
                if os.path.exists(link):
                    self.logger.warning('Removing the content of %s', link)
                    shutil.rmtree(link)
                os.symlink(path, link)
            else:
                self.logger.error('$HOME not found. Cannot create a link to the installation path')
                self.logger.warning('Tropical Rainfall will be installed in %s, but please remember to define TROPICAL_RAINFALL_CONFIG environment variable', path)
        else:
            self.logger.warning('Tropical Rainfall will be installed in %s, but please remember to define TROPICAL_RAINFALL_CONFIG environment variable', path)

    def add(self, args):
        """Placeholder for the 'add' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Adding something...')

    def remove(self, args):
        """Placeholder for the 'remove' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Removing something...')

    def set(self, args):
        """Placeholder for the 'set' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Setting something...')

    def uninstall(self, args):
        """Placeholder for the 'uninstall' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Uninstalling something...')

    def list(self, args):
        """Placeholder for the 'list' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Listing something...')

    def update(self, args):
        """Placeholder for the 'update' command

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Updating something...')

def main():
    """Tropical Rainfall main installation tool"""
    trcli = TropicalRainfallConsole()
    trcli.execute()

def query_yes_no(question, default="yes"):
    """Ask a yes/no question via input() and return their answer.

    Args:
        question (str): the question to be asked to the user
        default (str): the default answer if the user just hits <Enter>.
                       It must be "yes" (the default), "no" or None (meaning an answer is required of the user).

    Returns:
        bool: True for yes, False for no
    """
    valid = {"yes": True, "y": True, "ye": True, "no": False, "n": False}
    if default is None:
        prompt = " [y/n] "
    elif default == "yes":
        prompt = " [Y/n] "
    elif default == "no":
        prompt = " [y/N] "
    else:
        raise ValueError(f"invalid default answer: {default}")

    while True:
        sys.stdout.write(question + prompt)
        choice = input().lower()
        if default is not None and choice == "":
            return valid[default]
        elif choice in valid:
            return valid[choice]
        else:
            print("Please respond with 'yes' or 'no' (or 'y' or 'n').")

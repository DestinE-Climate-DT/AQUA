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
from tropical_rainfall import __path__ as pypath

class TropicalRainfallConsole():
    """Class for TropicalRainfallConsole, the Tropical Rainfall command line interface for
    initialization and configuration management"""

    def __init__(self):
        """The main Tropical Rainfall command line interface"""

        self.pypath = pypath[0]
        self.configpath = None
        self.logger = None

        self.command_map = {
            'init': self.init,
            'add_config': self.add_config,
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
        
        # Set the environment variable
        os.environ['TROPICAL_RAINFALL_CONFIG'] = self.configpath

    def _config_home(self):
        """Configure the Tropical Rainfall installation folder, by default inside $HOME"""

        if 'HOME' in os.environ:
            path = os.path.join(os.environ['HOME'], '.tropical_rainfall')
            self.configpath = path
            if not os.path.exists(path):
                os.makedirs(path, exist_ok=True)
            else:
                self.logger.warning('Tropical Rainfall already installed in %s', path)
                check = query_yes_no(f"Do you want to overwrite Tropical Rainfall installation in {path}. You will lose all configurations installed.", "no")
                if not check:
                    sys.exit()
                else:
                    self.logger.warning('Removing the content of %s', path)
                    shutil.rmtree(path)
                    os.makedirs(path, exist_ok=True)
        else:
            self.logger.error('$HOME not found. Please specify a path where to install Tropical Rainfall and define TROPICAL_RAINFALL_CONFIG as environment variable')
            sys.exit(1)
        
        # Set the environment variable
        os.environ['TROPICAL_RAINFALL_CONFIG'] = self.configpath

    def _config_path(self, path):
        """Define the Tropical Rainfall installation folder when a path is specified

        Args:
            path (str): the path where to install Tropical Rainfall
        """
        self.configpath = path
        if not os.path.exists(path):
            self.logger.info(f"Creating directory: {path}")
            os.makedirs(path, exist_ok=True)
        else:
            if not os.path.isdir(path):
                self.logger.error("Path chosen is not a directory")
                sys.exit(1)

        # Correctly determine the project root and path to the default configuration file
        script_dir = os.path.dirname(__file__)
        project_root = os.path.abspath(os.path.join(script_dir, '..', '..', '..'))
        default_config_file = os.path.join(project_root, 'diagnostics', 'tropical_rainfall', 'config', 'config-tropical-rainfall.yml')
        config_file = os.path.join(path, 'config-tropical-rainfall.yml')

        self.logger.info(f"Script directory: {script_dir}")
        self.logger.info(f"Project root directory: {project_root}")
        self.logger.info(f"Default config file path: {default_config_file}")
        self.logger.info(f"Target config file path: {config_file}")

        # Ensure the target directory exists before copying the configuration file
        target_dir = os.path.dirname(config_file)
        if not os.path.exists(target_dir):
            self.logger.info(f"Creating target directory: {target_dir}")
            os.makedirs(target_dir, exist_ok=True)

        if not os.path.exists(config_file):
            if os.path.exists(default_config_file):
                self.logger.info(f"Copying default config file from {default_config_file} to {config_file}")
                try:
                    shutil.copy(default_config_file, config_file)
                    self.logger.info(f"Configuration file created at {config_file}")
                except Exception as e:
                    self.logger.error(f"Failed to copy the configuration file: {e}")
                    sys.exit(1)
            else:
                self.logger.error(f"Default configuration file not found at {default_config_file}")
                sys.exit(1)
        else:
            self.logger.info(f"Configuration file already exists at {config_file}")

        check = query_yes_no(f"Do you want to create a link in the $HOME/.tropical_rainfall to {path}", "yes")
        if check:
            if 'HOME' in os.environ:
                link = os.path.join(os.environ['HOME'], '.tropical_rainfall')
                if os.path.exists(link):
                    self.logger.warning(f"Removing the content of {link}")
                    if os.path.islink(link):
                        os.unlink(link)
                    else:
                        shutil.rmtree(link)
                self.logger.info(f"Creating symlink from {link} to {path}")
                os.symlink(path, link)
            else:
                self.logger.error("$HOME not found. Cannot create a link to the installation path")
                self.logger.warning(f"Tropical Rainfall will be installed in {path}, but please remember to define TROPICAL_RAINFALL_CONFIG environment variable")
        else:
            self.logger.warning(f"Tropical Rainfall will be installed in {path}, but please remember to define TROPICAL_RAINFALL_CONFIG environment variable")

        # Set the environment variable
        os.environ['TROPICAL_RAINFALL_CONFIG'] = self.configpath
        self.logger.info(f"TROPICAL_RAINFALL_CONFIG environment variable set to {self.configpath}")


    def add_config(self, args):
        """Add a new configuration file to Tropical Rainfall

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        if self.configpath is None:
            self.logger.error("Configuration path is not set. Please run init command first.")
            return
        config_name = os.path.basename(args.config_name)
        config_src = os.path.abspath(args.config_name)
        config_dst = os.path.join(self.configpath, config_name)

        if not os.path.exists(config_src):
            self.logger.error(f"The configuration file {config_src} does not exist.")
            return

        shutil.copy(config_src, config_dst)
        self.logger.info(f"Configuration file {config_name} added to {self.configpath}.")

        # Update pyproject.toml to include the new configuration file
        pyproject_path = os.path.join(self.pypath, 'pyproject.toml')
        with open(pyproject_path, 'r') as file:
            pyproject_data = file.readlines()

        with open(pyproject_path, 'w') as file:
            for line in pyproject_data:
                file.write(line)
                if line.strip().startswith('tropical_rainfall ='):
                    file.write(f'    "{config_name}",\n')
        self.logger.info(f"pyproject.toml updated with the new configuration file {config_name}.")

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

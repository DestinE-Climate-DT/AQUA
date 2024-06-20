#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import shutil
import pytest
import sys
from aqua.util import load_yaml, dump_yaml, get_arg, query_yes_no, create_folder
from aqua.logger import log_configure
from aqua import __path__ as aqua_pypath
from tropical_rainfall import __path__ as pypath
from tropical_rainfall.cli.parser import parse_arguments

class TropicalRainfallConsole:
    """Class for TropicalRainfallConsole, the Tropical Rainfall command line interface for initialization and configuration management"""

    def __init__(self):
        self.configpath = None
        self.logger = None

        self.command_map = {
            'add_config': self.add_config,
            'run_cli': self.run_cli,
            'test': self.run_tests,  # Add new test command
        }

    def execute(self):
        parser_dict = parse_arguments()
        args = parser_dict['main'].parse_args(sys.argv[1:])

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

    def run_cli(self, args):
        """Run Tropical Rainfall CLI with the specified configuration file

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        nproc = args.nproc if args.nproc else 1
        config_file = args.config_file if args.config_file else os.path.join(pypath[0], 'cli', 'cli_config_trop_rainfall.yml')

        if not os.path.exists(config_file):
            self.logger.error(f"The configuration file {config_file} does not exist.")
            sys.exit(1)

        # Correct path to the CLI script within aqua/diagnostic/tropical_rainfall/cli/
        cli_script = os.path.join(aqua_pypath[0], 'diagnostics', 'tropical_rainfall', 'cli', 'cli_tropical_rainfall.py')
        if not os.path.exists(cli_script):
            self.logger.error(f"The CLI script {cli_script} does not exist.")
            sys.exit(1)

        cmd = f"python3 {cli_script} --config={config_file} --nproc={nproc}"
        self.logger.info(f"Running Tropical Rainfall CLI with {nproc} processes using config {config_file}")

        result = os.system(cmd)
        if result != 0:
            self.logger.error("Tropical Rainfall CLI execution failed")
            sys.exit(result)
        else:
            self.logger.info("Tropical Rainfall CLI executed successfully")

    def add_config(self, args):
        """Add and use a new configuration file for Tropical Rainfall

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Adding new configuration file for Tropical Rainfall')

        if args.config_file_path:
            config_name = os.path.basename(args.config_file_path)
            config_src = os.path.abspath(args.config_file_path)
        else:
            config_name = 'config-tropical-rainfall.yml'
            config_src = os.path.join(pypath[0], 'config', config_name)
            create_folder(os.path.join(pypath[0], 'config'))
            shutil.copy(os.path.join(pypath[0], config_name), config_src)

        self.logger.debug(f"Source config file path: {config_src}")
        if not os.path.exists(config_src):
            self.logger.error(f"The configuration file {config_src} does not exist.")
            sys.exit(1)

        config_dst = os.path.join(pypath[0], 'config', 'current_config.yml')
        self.logger.debug(f"Destination config file path: {config_dst}")
        target_dir = os.path.dirname(config_dst)

        create_folder(target_dir)

        # Always replace the existing current_config.yml
        if os.path.exists(config_dst):
            self.logger.debug(f"Removing existing config file: {config_dst}")
            os.remove(config_dst)

        shutil.copy(config_src, config_dst)
        self.logger.info(f"Configuration file {config_name} copied to {config_dst}")

        self.recompile_package()

    def run_tests(self, args):
        """Run tests for the Tropical Rainfall package

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        self.logger.info('Running tests for Tropical Rainfall package')

        # Get the .aqua configuration directory
        aqua_path = os.path.join(aqua_pypath[0], 'config')
        test_folder = os.path.join(aqua_path, 'tests') if not args.test_folder else args.test_folder

        test_args = ["-v", "-m", "tropical_rainfall", test_folder]
        result = pytest.main(test_args)
        if result == 0:
            self.logger.info("All tests passed successfully")
        else:
            self.logger.error("Some tests failed")
        sys.exit(result)

    def recompile_package(self):
        """Recompile the package to ensure new configurations are recognized."""
        try:
            import importlib
            import tropical_rainfall
            importlib.reload(tropical_rainfall)
            self.logger.info("Package recompiled successfully.")
        except Exception as e:
            self.logger.error(f"Failed to recompile the package: {e}")

def main():
    """Tropical Rainfall main installation tool"""
    trcli = TropicalRainfallConsole()
    trcli.execute()

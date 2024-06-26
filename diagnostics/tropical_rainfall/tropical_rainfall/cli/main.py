import os
import shutil
import sys
from aqua.util import create_folder
from aqua.logger import log_configure
from tropical_rainfall import __path__ as pypath
from tropical_rainfall.cli.parser import parse_arguments

class TropicalRainfallConsole:
    """Class for TropicalRainfallConsole, the Tropical Rainfall command line interface for initialization and configuration management"""

    def __init__(self, loglevel='WARNING'):
        self.configpath = None
        self.logger = log_configure(loglevel, 'Tropical Rainfall')

        self.command_map = {
            'add_config': self.add_config,
        }

    def execute(self):
        parser = parse_arguments()
        args = parser.parse_args(sys.argv[1:])

        if args.very_verbose or (args.verbose and args.very_verbose):
            loglevel = 'DEBUG'
        elif args.verbose:
            loglevel = 'INFO'
        else:
            loglevel = 'WARNING'
        self.logger = log_configure(loglevel, 'Tropical Rainfall')

        command = args.command
        method = self.command_map.get(command, parser.print_help)
        if command not in self.command_map:
            parser.print_help()
        else:
            method(args)

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line main functions
"""

from aqua.core.logger import log_configure

from .analysis import analysis_execute
from .builder import builder_execute
from .catalog import CatalogMixin
from .catgen import catgen_execute
from .components import discover_aqua_components
from .drop import drop_execute
from .files import FilesMixin

# this are used to check existence of aqua.diagnostics
from .install import InstallMixin
from .parser import parse_arguments
from .push_analysis import push_execute


class AquaConsole(InstallMixin, CatalogMixin, FilesMixin):
    """Class for AquaConsole, the AQUA command line interface for
    installation, catalog, grids, fixes and styles editing.

    Commands are dispatched by ``command_map``: a top-level command maps to its handler, a noun
    (``aqua <noun> <verb>``) to a dictionary of verb handlers."""

    def __init__(self):
        """The main AQUA command line interface"""

        # The configurer contains the ConfigPath class, while the configpath
        # will be populated by the configdir path by the _check method
        self.configurer = None
        self.configpath = None
        self.templatepath = None
        self.configfile = "config-aqua.yaml"
        self.grids = None
        self.logger = None
        self.loglevel = "WARNING"

        files = {"add": self.file_add, "remove": self.remove_file, "list": self.file_list}
        self.command_map = {
            "install": self.install,
            "uninstall": self.uninstall,
            "update": self.update,
            "list": self.list,
            "catalog": {
                "add": self.catalog_add,
                "remove": self.catalog_remove,
                "set": self.catalog_set,
                "update": self.catalog_update,
                "list": self.catalog_list,
                "avail": self.catalog_avail,
            },
            "grids": {
                **files,
                "path": self.grids_path,
                "build": self.grids_build,
                "deploy": self.grids_deploy,
            },
            "fixes": files,
            "styles": files,
            "analysis": self.analysis,
            "drop": self.drop,
            "catgen": self.catgen,
            "push": self.push,
        }

    def execute(self):
        """Parse AQUA class and run the required command"""

        parser_dict = parse_arguments()
        parser = parser_dict["main"]
        args = parser.parse_args()

        # Set the log level
        if args.very_verbose or (args.verbose and args.very_verbose):
            self.loglevel = "DEBUG"
        elif args.verbose:
            self.loglevel = "INFO"

        self.logger = log_configure(self.loglevel, "AQUA")

        self.components = discover_aqua_components()
        self.logger.debug(
            "Using aqua components: %s",
            [c for c in self.components if self.components[c]["installed"]],
        )
        self.aquapath = {name: info["path"] for name, info in self.components.items()}

        command = args.command
        if command not in self.command_map:
            parser.print_help()
            return None

        method = self.command_map[command]
        # nouns: aqua <noun> <verb>
        if isinstance(method, dict):
            if not args.nested_command:
                parser_dict[command].print_help()
                return None
            method = method[args.nested_command]
        return method(args)

    def analysis(self, args):
        """
        Run the AQUA analysis

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print("Running the AQUA analysis")
        analysis_execute(args)

    def push(self, args):
        """Publish AQUA analysis figures to a remote site."""
        return push_execute(args, logger=self.logger)

    def drop(self, args):
        """
        Run the Data Reduction OPerator

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print("Running the Data Reduction OPerator")
        drop_execute(args)

    def catgen(self, args):
        """
        Run the FDB catalog generator

        Args:
            args (argparse.Namespace): arguments from the command line
        """

        print("Running the catalog generator")
        catgen_execute(args, configcontext=self.configurer)

    def grids_build(self, args):
        """Build grids from data sources

        Args:
            args (argparse.Namespace): arguments from the command line
        """
        print("Running AQUA grids builder")
        builder_execute(args)


def main():
    """AQUA main installation tool"""
    aquacli = AquaConsole()
    return aquacli.execute()

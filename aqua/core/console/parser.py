#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line parser
"""

import argparse

from aqua import __version__ as version
from aqua.core.configurer import ConfigPackages
from aqua.core.console.analysis import analysis_parser
from aqua.core.console.builder import builder_parser
from aqua.core.console.catgen import catgen_parser
from aqua.core.console.components import discover_aqua_components
from aqua.core.console.drop import drop_parser
from aqua.core.console.push_analysis import push_parser


def parse_arguments():
    """Parse arguments for AQUA console"""

    parser = argparse.ArgumentParser(prog="aqua", description="AQUA command line tool")
    subparsers = parser.add_subparsers(dest="command", help="Available AQUA commands")

    # Parser for the aqua main command
    parser.add_argument(
        "--version", action="version", version=f"%(prog)s v{version}", help="show AQUA version number and exit."
    )
    parser.add_argument(
        "--path", action="version", version=f"{ConfigPackages().aqua_path}", help="show AQUA installation path and exit"
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Increase verbosity of the output to INFO loglevel")
    parser.add_argument(
        "-vv", "--very-verbose", action="store_true", help="Increase verbosity of the output to DEBUG loglevel"
    )

    # List of the subparsers with actions
    # Corresponding to the different aqua commands available (see command map)
    install_parser = subparsers.add_parser("install", description="Install AQUA configuration files")
    subparsers.add_parser("uninstall", description="Remove the current AQUA installation")
    subparsers.add_parser("update", description="Update the AQUA installation files not installed in editable mode")
    list_parser = subparsers.add_parser("list", description="List the current AQUA installation and its catalogs")

    # subparser for other AQUA commands as they are importing the parser from their code
    analysis_subparser = subparsers.add_parser("analysis", description="Run AQUA diagnostics")
    analysis_subparser = analysis_parser(parser=analysis_subparser)

    drop_subparser = subparsers.add_parser("drop", description="Data Reduction OPerator")
    drop_subparser = drop_parser(parser=drop_subparser)

    catgen_subparser = subparsers.add_parser("catgen", description="FDB catalog generator")
    catgen_subparser = catgen_parser(parser=catgen_subparser)

    push_subparser = subparsers.add_parser("push", description="Publish AQUA analysis figures to a remote site.")
    push_parser(parser=push_subparser)

    # subparsers for catalogs: aqua catalog <verb>
    parser_catalog = subparsers.add_parser("catalog", help="catalog related commands")
    catalog_subparsers = parser_catalog.add_subparsers(dest="nested_command")
    catalog_add_parser = catalog_subparsers.add_parser("add", help="Add a catalog in the current AQUA installation")
    catalog_remove_parser = catalog_subparsers.add_parser("remove", help="Remove a catalog in the current AQUA installation")
    catalog_set_parser = catalog_subparsers.add_parser(
        "set", help="Set an installed catalog as the predefined in config-aqua.yaml"
    )
    catalog_update_parser = catalog_subparsers.add_parser("update", help="Update a catalog in the current AQUA installation")
    catalog_subparsers.add_parser("list", help="List the currently installed AQUA catalogs")
    catalog_avail_parser = catalog_subparsers.add_parser("avail", help="List the ClimateDT available catalogs on GitHub")

    # subparsers for projects: aqua project <verb>
    parser_project = subparsers.add_parser("project", help="project related commands")
    project_subparsers = parser_project.add_subparsers(dest="nested_command")
    project_install_parser = project_subparsers.add_parser("install", help="Install a project")
    project_update_parser = project_subparsers.add_parser("update", help="Update a project not installed in editable mode")
    project_remove_parser = project_subparsers.add_parser("remove", help="Remove an installed project")
    project_set_parser = project_subparsers.add_parser(
        "set", help="Set an installed project as the active one in config-aqua.yaml"
    )
    project_subparsers.add_parser("list", help="List the installed projects, marking the active one")

    # subparsers for grids, fixes and styles
    parser_grids = file_subparser(subparsers, "grids")
    parser_fixes = file_subparser(subparsers, "fixes")
    parser_styles = file_subparser(subparsers, "styles")

    # extra parsers arguments
    install_parser.add_argument("machine", metavar="MACHINE_NAME", help="Machine on which install AQUA")
    install_parser.add_argument(
        "-p", "--path", type=str, metavar="AQUA_TARGET_PATH", help="Path where to install AQUA. Default is $HOME/.aqua"
    )
    # Only core and diagnostics have a one-letter flag: a plugin starting with "c" would collide with core
    short_flags = {"core": ["-c"], "diagnostics": ["-d"]}
    for name in discover_aqua_components():
        install_parser.add_argument(
            *short_flags.get(name, []),
            f"--{name}",
            nargs="?",
            const="standard",
            type=str,
            metavar=f"AQUA_{name.upper()}_PATH",
            help=f"Install AQUA {name}. Without path: standard installation of {name} only. "
            "With path: editable installation from that path",
        )

    list_parser.add_argument(
        "-a", "--all", action="store_true", help="Print also all the installed fixes, grids and data_models"
    )

    catalog_add_parser.add_argument("catalog", metavar="CATALOG_NAME", help="Catalog to be installed")
    catalog_add_parser.add_argument(
        "-e", "--editable", type=str, help="Install a catalog in editable mode from the original source: provide the Path"
    )
    catalog_add_parser.add_argument(
        "-r", "--repository", type=str, help="Install a catalog from a specific repository: provide the user/repo string"
    )

    catalog_remove_parser.add_argument("catalog", metavar="CATALOG_NAME", help="Catalog to be removed")

    catalog_set_parser.add_argument("catalog", metavar="CATALOG_NAME", help="Catalog to be used in AQUA")

    catalog_update_parser.add_argument(
        "catalog", metavar="CATALOG_NAME", help="Catalog to be updated, or 'all' for every installed catalog"
    )

    catalog_avail_parser.add_argument(
        "-r", "--repository", type=str, help="Explore a specific repository: provide the user/repo string"
    )

    project_install_parser.add_argument("project", metavar="PROJECT_NAME", help="Project to be installed")
    project_install_parser.add_argument(
        "-e", "--editable", type=str, help="Install a project in editable mode from the original source: provide the Path"
    )
    project_update_parser.add_argument("project", metavar="PROJECT_NAME", help="Project to be updated")
    project_remove_parser.add_argument("project", metavar="PROJECT_NAME", help="Project to be removed")
    project_set_parser.add_argument("project", metavar="PROJECT_NAME", help="Project to be used in AQUA")

    # create a dictionary to simplify the call
    parser_dict = {
        "main": parser,
        "catalog": parser_catalog,
        "project": parser_project,
        "fixes": parser_fixes,
        "grids": parser_grids,
        "styles": parser_styles,
    }

    return parser_dict


def file_subparser(main_parser, name):
    """Compact subparsers for file handling - fixes, grids and styles"""

    # subparsers for fixes, grids and styles
    parser = main_parser.add_parser(name, help=f"{name} related commands")
    subparsers = parser.add_subparsers(dest="nested_command")

    parser_add = subparsers.add_parser("add", help=f"Add a {name} file in the current AQUA installation")
    parser_add.add_argument("file", help=f"The {name} file to add")
    parser_add.add_argument(
        "-e", "--editable", action="store_true", help=f"Add a {name} file in editable mode from the original path"
    )
    parser_remove = subparsers.add_parser("remove", help=f"Remove a {name} file")
    parser_remove.add_argument("file", help=f"The {name} file to remove")
    subparsers.add_parser("list", help=f"List the installed {name} files")

    # We have for the grids the possibility to set a default path to overwrite the individual catalog one
    # This will create a block in the config-aqua.yaml file for grids, areas and weights.
    if name == "grids":
        parser_path = subparsers.add_parser("path", help=f"Set a {name} path as the default in config-aqua.yaml")
        parser_path.add_argument("path", help=f"The {name} path to set as default")
        parser_build = subparsers.add_parser("build", help=f"Build {name} grids from data sources")
        parser_build = builder_parser(parser_build)
        parser_deploy = subparsers.add_parser(
            "deploy",
            help=f"Deploy {name} grids. Requires to set the default path with the aqua grids path command",
        )  # noqa: E501
        parser_deploy.add_argument(
            "source_grid_name", help="The source grid name to deploy. Can be a wildcard to deploy all the matching grids"
        )

    return parser

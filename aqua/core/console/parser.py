#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line parser.

The console follows a noun-first grammar, ``aqua <noun> <verb> [args]`` (e.g. ``aqua catalog add ci``),
plus a few top-level commands (``install``, ``uninstall``, ``update``, ``list``, ``analysis``, ``drop``,
``catgen``, ``push``). A new noun is added with :func:`add_noun` and registered in the dictionary returned
by :func:`parse_arguments`; its verbs are dispatched by ``AquaConsole.command_map`` in ``main.py``.

The verb-first commands of AQUA < 1.3 (``aqua add``, ``aqua remove``, ``aqua set``, ``aqua avail``,
``aqua update -c`` and ``aqua grids set``) are kept as deprecated aliases until AQUA 2.0.
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

# Components keeping a one-letter install flag for backward compatibility: every other plugin gets the
# long flag only, since two plugin names with the same initial (or starting with "c") would collide.
SHORT_INSTALL_FLAGS = {"core": "-c", "diagnostics": "-d"}

# Nouns managing files that can be added to the AQUA configuration folder of the same name
FILE_NOUNS = ["grids", "fixes", "styles"]


def parse_arguments():
    """Build the AQUA console parser.

    Returns:
        dict: the main parser under ``"main"`` and one parser per noun (used to print the noun help
        when the verb is missing).
    """

    parser = argparse.ArgumentParser(prog="aqua", description="AQUA command line tool")
    subparsers = parser.add_subparsers(dest="command", metavar="COMMAND", title="commands")

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

    # top-level commands
    install_parser(subparsers)
    subparsers.add_parser("uninstall", help="Remove the current AQUA installation")
    update_parser = subparsers.add_parser(
        "update", help="Update the AQUA installation files of every component not installed in editable mode"
    )
    update_parser.add_argument(
        "-c", "--catalog", type=str, help="Deprecated, use `aqua catalog update`: update a catalog (or 'all')"
    )
    list_parser = subparsers.add_parser("list", help="Show the AQUA installation and the installed catalogs")
    list_parser.add_argument(
        "-a", "--all", action="store_true", help="Print also all the installed fixes, grids and data_models"
    )

    analysis_parser(parser=subparsers.add_parser("analysis", help="Run AQUA diagnostics"))
    drop_parser(parser=subparsers.add_parser("drop", help="Data Reduction OPerator"))
    catgen_parser(parser=subparsers.add_parser("catgen", help="FDB catalog generator"))
    push_parser(parser=subparsers.add_parser("push", help="Publish AQUA analysis figures to a remote site"))

    # nouns
    parser_dict = {"main": parser, "catalog": catalog_subparser(subparsers), "project": project_subparser(subparsers)}
    for name in FILE_NOUNS:
        parser_dict[name] = file_subparser(subparsers, name)

    # deprecated verb-first catalog commands: no help, so they are not listed in `aqua -h`
    add_catalog_add_arguments(subparsers.add_parser("add", description="Deprecated, use `aqua catalog add`"))
    add_catalog_name_argument(
        subparsers.add_parser("remove", description="Deprecated, use `aqua catalog remove`"), "Catalog to be removed"
    )
    add_catalog_name_argument(
        subparsers.add_parser("set", description="Deprecated, use `aqua catalog set`"), "Catalog to be used in AQUA"
    )
    add_repository_argument(subparsers.add_parser("avail", description="Deprecated, use `aqua catalog avail`"))

    return parser_dict


def add_noun(subparsers, name, help_text):
    """Add a noun, i.e. a command of the form ``aqua <name> <verb>``.

    Args:
        subparsers (argparse._SubParsersAction): the subparsers of the main parser.
        name (str): the noun.
        help_text (str): one-line description shown in ``aqua -h``.

    Returns:
        tuple: the noun parser and its verb subparsers; ``verbs.add_parser(<verb>)`` adds a verb, whose
        name is stored in ``args.nested_command``.
    """
    noun = subparsers.add_parser(name, help=help_text, description=help_text)
    verbs = noun.add_subparsers(dest="nested_command", metavar="VERB", title="verbs")
    return noun, verbs


def install_parser(subparsers):
    """Add ``aqua install``, with one flag per discovered component."""
    parser = subparsers.add_parser("install", help="Install the AQUA configuration files")
    parser.add_argument("machine", metavar="MACHINE_NAME", help="Machine on which install AQUA")
    parser.add_argument(
        "-p", "--path", type=str, metavar="AQUA_TARGET_PATH", help="Path where to install AQUA. Default is $HOME/.aqua"
    )
    for name in discover_aqua_components():
        flags = [SHORT_INSTALL_FLAGS[name]] if name in SHORT_INSTALL_FLAGS else []
        parser.add_argument(
            *flags,
            f"--{name}",
            nargs="?",
            const="standard",
            type=str,
            metavar=f"AQUA_{name.upper()}_PATH",
            help=f"Install AQUA {name}. Without path: standard installation of {name} only. "
            "With path: editable installation from that path",
        )
    return parser


def add_catalog_name_argument(parser, help_text):
    """Add the positional catalog name."""
    parser.add_argument("catalog", metavar="CATALOG_NAME", help=help_text)


def add_repository_argument(parser):
    """Add the -r/--repository option for commands reading from GitHub."""
    parser.add_argument("-r", "--repository", type=str, help="Use a specific GitHub repository: provide the user/repo string")


def add_catalog_add_arguments(parser):
    """Add the arguments of ``aqua catalog add``."""
    add_catalog_name_argument(parser, "Catalog to be installed")
    parser.add_argument(
        "-e", "--editable", type=str, help="Install a catalog in editable mode from the original source: provide the Path"
    )
    add_repository_argument(parser)


def catalog_subparser(subparsers):
    """Add the ``catalog`` noun."""
    parser, verbs = add_noun(subparsers, "catalog", "Install, select and update AQUA catalogs")

    add_catalog_add_arguments(verbs.add_parser("add", help="Add a catalog and set it as the default"))
    add_catalog_name_argument(verbs.add_parser("remove", help="Remove an installed catalog"), "Catalog to be removed")
    add_catalog_name_argument(
        verbs.add_parser("set", help="Set an installed catalog as the default in config-aqua.yaml"),
        "Catalog to be used in AQUA",
    )
    add_catalog_name_argument(
        verbs.add_parser("update", help="Download again a catalog not installed in editable mode"),
        "Catalog to be updated, or 'all' for every installed catalog",
    )
    verbs.add_parser("list", help="List the installed catalogs")
    add_repository_argument(verbs.add_parser("avail", help="List the catalogs available on GitHub"))
    return parser


def project_subparser(subparsers):
    """Add the ``project`` noun (the name "project" is provisional)."""
    parser, verbs = add_noun(subparsers, "project", "Install, select and update AQUA projects")

    install = verbs.add_parser("install", help="Install a project")
    install.add_argument("project", metavar="PROJECT_NAME", help="Project to be installed")
    install.add_argument(
        "-e", "--editable", type=str, help="Install a project in editable mode from the original source: provide the Path"
    )
    for verb, help_text in (
        ("update", "Update the files of a project not installed in editable mode"),
        ("remove", "Remove an installed project"),
        ("set", "Set an installed project as the active one in config-aqua.yaml"),
    ):
        verbs.add_parser(verb, help=help_text).add_argument("project", metavar="PROJECT_NAME", help=f"Project to {verb}")
    verbs.add_parser("list", help="List the installed projects, marking the active one")
    return parser


def file_subparser(subparsers, name):
    """Add a noun handling the files of an AQUA configuration folder: ``grids``, ``fixes`` or ``styles``."""
    parser, verbs = add_noun(subparsers, name, f"{name} related commands")

    parser_add = verbs.add_parser("add", help=f"Add a {name} file in the current AQUA installation")
    parser_add.add_argument("file", help=f"The {name} file to add")
    parser_add.add_argument(
        "-e", "--editable", action="store_true", help=f"Add a {name} file in editable mode from the original path"
    )
    parser_remove = verbs.add_parser("remove", help=f"Remove a {name} file")
    parser_remove.add_argument("file", help=f"The {name} file to remove")
    verbs.add_parser("list", help=f"List the installed {name} files")

    # The grids can have a default path overriding the one of the individual catalogs:
    # it creates a block in config-aqua.yaml for grids, areas and weights.
    if name == "grids":
        path_help = "Set a grids path (with grids, areas and weights subfolders) as the default in config-aqua.yaml"
        verbs.add_parser("path", help=path_help).add_argument("path", help="The grids path to set as default")
        # deprecated alias of `grids path`: no help, so it is not listed
        verbs.add_parser("set", description="Deprecated, use `aqua grids path`").add_argument(
            "path", help="The grids path to set as default"
        )
        builder_parser(verbs.add_parser("build", help="Build grids from data sources"))
        verbs.add_parser(
            "deploy", help="Deploy grids. Requires to set the default path with the `aqua grids path` command"
        ).add_argument(
            "source_grid_name", help="The source grid name to deploy. Can be a wildcard to deploy all the matching grids"
        )

    return parser

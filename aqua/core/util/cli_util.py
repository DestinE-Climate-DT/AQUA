"""CLI utility functions for AQUA command line interfaces."""

import argparse


def template_parse_arguments(parser: argparse.ArgumentParser):
    """
    Add the default arguments to the parser.

    Args:
        parser: argparse.ArgumentParser

    Returns:
        argparse.ArgumentParser
    """
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")
    parser.add_argument("--catalog", type=str,
                        required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--realization", type=str, default=None,
                        help="realization name (default: None)")
    parser.add_argument("--config", "-c", type=str, default=None,
                        help='yaml configuration file')
    parser.add_argument("--nworkers", "-n", type=int,
                        required=False, help="number of workers")
    parser.add_argument("--cluster", type=str,
                        required=False, help="cluster address")
    parser.add_argument("--regrid", type=str,
                        required=False, help="target regrid resolution")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")
    parser.add_argument("--startdate", type=str,
                        required=False, help="start date (YYYY-MM-DD)")
    parser.add_argument("--enddate", type=str,
                        required=False, help="end date (YYYY-MM-DD)")

    return parser


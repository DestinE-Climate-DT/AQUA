#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AQUA CLI wrapper for the FDB catalog generator.
"""

import argparse
import sys

from aqua import AquaFDBGenerator
from aqua.core.util import get_arg


def catgen_parser(parser=None):
    """Create the argument parser for the FDB catalog generator"""
    if parser is None:
        parser = argparse.ArgumentParser(description="AQUA FDB entries generator")

    parser.add_argument("-p", "--portfolio", help="Type of Data Portfolio utilized (full/reduced/minimal)")
    parser.add_argument("-c", "--config", type=str, help="yaml configuration file", required=True)
    parser.add_argument("-l", "--loglevel", type=str, help="loglevel", default="INFO")

    return parser


def catgen_execute(args):
    """Useful wrapper for the FDB catalog generator class"""

    dp_version = get_arg(args, "portfolio", "full")
    config_file = get_arg(args, "config", "config.yaml")
    loglevel = get_arg(args, "loglevel", "INFO")

    generator = AquaFDBGenerator(dp_version, config_file, loglevel)
    generator.generate_catalog()


if __name__ == "__main__":
    args = catgen_parser().parse_args(sys.argv[1:])
    catgen_execute(args)

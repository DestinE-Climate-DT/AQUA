#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
AQUA CLI wrapper for the FDB catalog generator.
"""

import argparse
import sys

from aqua import AquaSTACGenerator
from aqua.core.util import get_arg

BRIDGE_API_URL = {
    "lumi": "https://qubed.lumi.apps.dte.destination-earth.eu/api/v2/stac",
    "MN5": None,
}

DEFAULT_LAYERS = ["activity", "experiment", "model", "realization", "expver", "stream", "resolution", "levtype"]

DEFAULT_FILTERS = {
    "activity": ["baseline", "projections"],
    "model": ["icon", "ifs-fesom"],
    "expver": "0001",
    "stream": ["clte"],
}


def stacgen_parser(parser=None):
    """Create the argument parser for the STAC catalog generator"""
    if parser is None:
        parser = argparse.ArgumentParser(description="AQUA STAC entries generator")

    parser.add_argument("-b", "--bridge", help="bridge node", choices=BRIDGE_API_URL.keys())
    parser.add_argument("-c", "--catalog", type=str, help="target catalog")
    parser.add_argument("-l", "--loglevel", type=str, help="loglevel")
    parser.add_argument("-o", "--output", type=str, help="output directory")
    parser.add_argument("--model", type=str, help="model to filter")
    parser.add_argument("--activity", type=str, help="activity to filter")

    return parser


def stacgen_execute(args):
    """Useful wrapper for the STAC catalog generator class"""

    bridge = get_arg(args, "bridge", "lumi")
    catalog = get_arg(args, "catalog", "climate-dt-gen2")
    loglevel = get_arg(args, "loglevel", "INFO")
    output = get_arg(args, "output", ".")
    model = get_arg(args, "model", DEFAULT_FILTERS["model"])
    activity = get_arg(args, "activity", DEFAULT_FILTERS["activity"])

    if bridge not in BRIDGE_API_URL:
        raise ValueError(f"Unsupported bridge node: {bridge}")

    # setup filters for the catalog generation
    filters = DEFAULT_FILTERS.copy()
    filters["model"] = [model]
    filters["activity"] = [activity]

    generator = AquaSTACGenerator(catalog=catalog, loglevel=loglevel, bridge_url=BRIDGE_API_URL[bridge])
    generator.explore_tree(layers=DEFAULT_LAYERS, filters=filters)
    generator.complete_request()
    generator.generate_catalog(catalog_dir_path=output, catalog_name=f"{catalog}-stac")


if __name__ == "__main__":
    args = stacgen_parser().parse_args(sys.argv[1:])
    stacgen_execute(args)

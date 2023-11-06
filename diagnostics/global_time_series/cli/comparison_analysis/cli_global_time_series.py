#!/usr/bin/env python3
"""Command-line interface for global time series diagnostic.
"""
import argparse
import sys
import matplotlib.pyplot as plt
from aqua.util import load_yaml
sys.path.insert(0, "../../")
from global_time_series import plot_timeseries, plot_gregory


def _main():
    parser = argparse.ArgumentParser(description="Global time series CLI")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="yaml configuration file",
        default="config_time_series.yaml",
    )
    args = parser.parse_args()
    config = load_yaml(args.config)

    if "timeseries" in config:
        fig, ax = plt.subplots()
        for src_config in config["timeseries"]["sources"].values():
            plot_timeseries(**src_config, ax=ax)
        if "savefig" in config["timeseries"]:
            fig.savefig(config["timeseries"]["savefig"])

    if "gregory" in config:
        fig, ax = plt.subplots()
        plot_gregory(**config["gregory"], ax=ax)
        if "savefig" in config["gregory"]:
            fig.savefig(config["gregory"]["savefig"])


if __name__ == "__main__":
    _main()

#!/usr/bin/env python3
"""
Command-line interface for global time series diagnostic.

This CLI allows to plot timeseries for a set of models and experiments
and a set of variables defined in a yaml configuration file.
"""
import argparse
import sys
import matplotlib.pyplot as plt
sys.path.insert(0, "../../../") 
from aqua.util import load_yaml
from aqua.exceptions import NotEnoughDataError
sys.path.insert(0, "../../") 
from global_time_series import plot_timeseries, plot_gregory

def create_plots_filename(config=None, var=None):
    """Create a filename for the plots."""
    if config is None:
        raise ValueError("No config provided.")
    if var is None:
        raise ValueError("No variable provided.")

    diagnostic = "global_time_series"
    filename = f"{diagnostic}"

    for src_config in config["timeseries"]["sources"].values():
        filename += f"_{src_config['model']}_{src_config['exp']}"
        source = src_config['reader_kw']['source']
        filename += f"_{source}"
    filename += f"_{var}"
    filename += ".pdf"

    return filename

def create_outfile_filename(config=None, var=None):
    """Create a filename for the output file."""
    if config is None:
        raise ValueError("No config provided.")
    if var is None:
        raise ValueError("No variable provided.")

    diagnostic = "global_time_series"
    filename = f"{diagnostic}"
    filename += f"_{config['model']}_{config['exp']}"
    source = config['reader_kw']['source']
    filename += f"_{source}"
    filename += f"_{var}"
    filename += ".nc"

    return filename


def _main():
    parser = argparse.ArgumentParser(description="Global time series CLI")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        help="yaml configuration file",
        default="config_time_series.yaml",
    )
    parser.add_argument(
        "-v",
        "--verbose",
        action="store_true",
        help="verbose mode",
        default=False,
    )
    args = parser.parse_args()
    config = load_yaml(args.config)
    verbose = args.verbose

    if "timeseries" in config:
        if verbose:
            print("Plotting timeseries")
        vars = config["timeseries"]["variables"]
        if verbose:
            print(f"Variables: {vars}")
        for var in vars:
            if verbose:
                print(f"Plotting timeseries for {var}")
            fig, ax = plt.subplots()
            for src_config in config["timeseries"]["sources"].values():
                outfile = create_outfile_filename(config=src_config, var=var)
                if verbose:
                    print(f"Saving data to {outfile}")
                try:
                    plot_timeseries(**src_config, variable=var, ax=ax, outfile=outfile)
                except NotEnoughDataError as e:                  
                    print(f"Error: {e}")
                    exit(0)
                plot_timeseries(**src_config, variable=var, ax=ax, outfile=outfile)
            if "savefig" in config["timeseries"]:
                filename = create_plots_filename(config=config, var=var)
                if verbose:
                    print(f"Saving figure to {filename}")
                fig.savefig(filename)

    if "gregory" in config:
        if verbose:
            print("Plotting Gregory plot")
        fig, ax = plt.subplots()
        plot_gregory(**config["gregory"], ax=ax)
        if "savefig" in config["gregory"]:
            filename = create_plots_filename(config=config, var="gregory_plot")
            if verbose:
                print(f"Saving figure to {filename}")
            fig.savefig(filename)

    if verbose:
        print("Done")


if __name__ == "__main__":
    _main()

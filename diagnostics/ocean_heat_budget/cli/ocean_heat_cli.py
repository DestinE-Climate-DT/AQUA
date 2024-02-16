#!/usr/bin/env python3
"""
Command-line interface for global time series diagnostic.

This CLI allows to plot timeseries of a set of variables
defined in a yaml configuration file for a single experiment
and gregory plot.
"""
import argparse
from aqua import Reader
import numpy as np
from ocean_heat_functions import compute_net_surface_fluxes, plot_time_series


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Global time series CLI")

    parser.add_argument("-c", "--config",
                        type=str, required=False,
                        help="yaml configuration file")
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")

    # These will override the ones in the config file if provided
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser.parse_args(args)


def create_filename(outputdir=None, plotname=None, type=None,
                    model=None, exp=None, source=None, resample=None):
    """
    Create a filename for the plots

    Args:
        outputdir (str): output directory
        plotname (str): plot name
        type (str): type of output file (pdf or nc)
        model (str): model name
        exp (str): experiment name
        source (str): source name
        resample (str): resample frequency

    Returns:
        filename (str): filename

    Raises:
        ValueError: if no output directory is provided
        ValueError: if no plotname is provided
        ValueError: if type is not pdf or nc
    """
    if outputdir is None:
        print("No output directory provided, using current directory.")
        outputdir = "."

    if plotname is None:
        raise ValueError("No plotname provided.")

    if type != "pdf" and type != "nc":
        raise ValueError("Type must be pdf or nc.")

    diagnostic = "global_time_series"
    filename = f"{diagnostic}"
    filename += f"_{model}_{exp}_{source}"
    filename += f"_{plotname}"

    if resample == 'YS':
        filename += "_annual"

    if type == "pdf":
        filename += ".pdf"
    elif type == "nc":
        filename += ".nc"

    return filename

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    # Loglevel settings
    loglevel = get_arg(args, 'loglevel', 'WARNING')

    logger = log_configure(log_level=loglevel,
                           log_name='CLI Global Time Series')

    logger.info('Running global time series diagnostic...')

    # we change the current directory to the one of the CLI
    # so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # import the functions from the diagnostic now that
    # we are in the right directory
    sys.path.insert(0, "../../../")
    from global_time_series import plot_timeseries, plot_gregory

    # Read configuration file
    file = get_arg(args, 'config', 'config_time_series_atm.yaml')
    logger.info(f"Reading configuration yaml file: {file}")
    config = load_yaml(file)

    model = get_arg(args, 'model', config['model'])
    exp = get_arg(args, 'exp', config['exp'])
    source = get_arg(args, 'source', config['source'])

    outputdir = get_arg(args, 'outputdir', config['outputdir'])

    logger.debug(f"model: {model}")
    logger.debug(f"exp: {exp}")
    logger.debug(f"source: {source}")
    logger.debug(f"outputdir: {outputdir}")

    outputdir_nc = os.path.join(outputdir, "netcdf")
    create_folder(folder=outputdir_nc, loglevel=loglevel)
    outputdir_pdf = os.path.join(outputdir, "pdf")
    create_folder(folder=outputdir_pdf, loglevel=loglevel)
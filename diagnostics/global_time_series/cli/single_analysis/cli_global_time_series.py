#!/usr/bin/env python3
"""
Command-line interface for global time series diagnostic.

This CLI allows to plot timeseries of a set of variables 
defined in a yaml configuration file for a single experiment
and gregory plot.
"""
import argparse
import os
import sys
import matplotlib.pyplot as plt

from aqua.util import load_yaml, get_arg, create_folder
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure

# sys.path.insert(0, "../../")
# from global_time_series import plot_timeseries, plot_gregory


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Global time series CLI")

    parser.add_argument("-c", "--config",
                        type=str, required=False,
                        help="yaml configuration file")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")

    return parser.parse_args(args)


def create_filename(outputdir=None, plotname=None, type=None,
                    model=None, exp=None, source=None):
    """
    Create a filename for the plots

    Args:
        outputdir (str): output directory
        plotname (str): plot name
        type (str): type of output file (pdf or nc)
        model (str): model name
        exp (str): experiment name
        source (str): source name

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

    if type == "pdf":
        filename += ".pdf"
    elif type == "nc":
        filename += ".nc"

    return filename


if __name__ == '__main__':

    print('Running global time series diagnostic...')
    args = parse_arguments(sys.argv[1:])

    # Loglevel settings
    loglevel = get_arg(args, 'loglevel', 'WARNING')

    logger = log_configure(log_level=loglevel,
                           log_name='CLI Global Time Series')

    # we change the current directory to the one of the CLI
    # so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f'Moving from current directory to {dname} to run!')

    # import the functions from the diagnostic now that
    # we are in the right directory
    sys.path.insert(0, "../../")
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

    outputdir_nc = os.path.join(outputdir, "NetCDF")
    create_folder(folder=outputdir_nc, loglevel=loglevel)
    outputdir_pdf = os.path.join(outputdir, "pdf")
    create_folder(folder=outputdir_pdf, loglevel=loglevel)

    if "timeseries" in config:
        logger.warning("Plotting timeseries...")

        for var in config["timeseries"]:
            logger.warning(f"Plotting {var}")

            # Creating the output filename
            filename_nc = create_filename(outputdir=outputdir,
                                          plotname=var, type="nc",
                                          model=model, exp=exp,
                                          source=source)
            filename_nc = os.path.join(outputdir_nc, filename_nc)
            logger.info(f"Output file: {filename_nc}")

            # Reading the configuration file
            try:
                plot_kw = config["timeseries"][var]["plot_kw"]
            except KeyError:
                plot_kw = None
            try:
                plot_era5 = config["timeseries"][var]["plot_era5"]
            except KeyError:
                plot_era5 = False
            try:
                resample = config["timeseries"][var]["resample"]
            except KeyError:
                logger.warning("No resample rate provided, using monthly.")
                resample = "M"
            try:
                ylim = config["timeseries"][var]["ylim"]
            except KeyError:
                ylim = {}
            try:
                reader_kw = config["timeseries"][var]["reader_kw"]
            except KeyError:
                reader_kw = {}  # empty dict, source will be added later
            # add source to reader_kw
            reader_kw["source"] = source

            # Generating the image
            fig, ax = plt.subplots()

            try:
                plot_timeseries(model=model, exp=exp, variable=var,
                                resample=resample, plot_era5=plot_era5,
                                ylim=ylim, plot_kw=plot_kw, ax=ax,
                                reader_kw=reader_kw, outfile=filename_nc,
                                loglevel=loglevel)
            except (NotEnoughDataError, NoDataError, NoObservationError) as e:
                logger.error(f"Error: {e}")
                continue
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error("This is a bug, please report it.")
                continue

            if "savefig" in config["timeseries"][var]:
                filename_pdf = create_filename(outputdir=outputdir,
                                               plotname=var, type="pdf",
                                               model=model, exp=exp,
                                               source=source)
                filename_pdf = os.path.join(outputdir_pdf, filename_pdf)
                logger.info(f"Output file: {filename_pdf}")
                fig.savefig(filename_pdf)

    if "gregory" in config:
        logger.warning("Plotting gregory plot...")

        # Creating the output filename
        filename_nc = create_filename(outputdir=outputdir,
                                      plotname="gregory", type="nc",
                                      model=model, exp=exp,
                                      source=source)
        filename_nc = os.path.join(outputdir_nc, filename_nc)
        logger.info(f"Output file: {filename_nc}")

        # Generating the image
        fig, ax = plt.subplots()

        try:
            plot_kw = config["gregory"]["plot_kw"]
        except KeyError:
            plot_kw = None
        try:
            resample = config["gregory"]["resample"]
        except KeyError:
            logger.warning("No resample rate provided, using monthly.")
            resample = "M"
        try:
            reader_kw = config["gregory"]["reader_kw"]
        except KeyError:
            reader_kw = {}
        # add source to reader_kw
        reader_kw["source"] = source

        try:
            plot_gregory(model=model, exp=exp, reader_kw=reader_kw,
                        plot_kw=plot_kw, ax=ax, outfile=filename_nc,
                        freq=resample)
        except (NotEnoughDataError, NoDataError) as e:
            logger.error(f"Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error("This is a bug, please report it.")

        if "savefig" in config["gregory"]:
            filename_pdf = create_filename(outputdir=outputdir,
                                           plotname="gregory", type="pdf",
                                           model=model, exp=exp,
                                           source=source)
            filename_pdf = os.path.join(outputdir_pdf, filename_pdf)
            logger.info(f"Output file: {filename_pdf}")
            fig.savefig(filename_pdf)

    logger.info("Analysis completed.")

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

    if "timeseries" in config:
        logger.info("Plotting timeseries")

        for var in config["timeseries"]:
            logger.info(f"Plotting {var}")

            # Creating the output filename
            filename_nc = create_filename(outputdir=outputdir,
                                          plotname=var, type="nc",
                                          model=model, exp=exp,
                                          source=source)
            filename_nc = os.path.join(outputdir_nc, filename_nc)
            logger.info(f"Output file: {filename_nc}")

            # Reading the configuration file
            plot_options = config["timeseries_plot_params"].get(var)
            if plot_options:
                plot_kw = plot_options.get("plot_kw", None)
                plot_era5 = plot_options.get("plot_era5", False)
                resample = plot_options.get("resample", "M")
                ylim = plot_options.get("ylim", {})
                reader_kw = plot_options.get("reader_kw", {})
                savefig = plot_options.get("savefig", True)
                annual = plot_options.get("annual", True)
                startdate = plot_options.get("startdate", None)
                enddate = plot_options.get("enddate", None)
                std_startdate = plot_options.get("std_startdate", "1991-01-01")
                std_enddate = plot_options.get("std_enddate", "2020-12-31")
                monthly_std = plot_options.get("monthly_std", True)
                annual_std = plot_options.get("annual_std", True)
            else:  # default
                plot_kw = config["timeseries_plot_params"]["default"].get("plot_kw", None)
                plot_era5 = config["timeseries_plot_params"]["default"].get("plot_era5", False)
                resample = config["timeseries_plot_params"]["default"].get("resample", "M")
                ylim = config["timeseries_plot_params"]["default"].get("ylim", {})
                reader_kw = config["timeseries_plot_params"]["default"].get("reader_kw", {})
                savefig = config["timeseries_plot_params"]["default"].get("savefig", True)
                annual = config["timeseries_plot_params"]["default"].get("annual", True)
                startdate = config["timeseries_plot_params"]["default"].get("startdate", None)
                enddate = config["timeseries_plot_params"]["default"].get("enddate", None)
                std_startdate = config["timeseries_plot_params"]["default"].get("std_startdate", "1991-01-01")
                std_enddate = config["timeseries_plot_params"]["default"].get("std_enddate", "2020-12-31")
                monthly_std = config["timeseries_plot_params"]["default"].get("monthly_std", True)
                annual_std = config["timeseries_plot_params"]["default"].get("annual_std", True)

            # Generating the image
            fig, ax = plt.subplots(figsize=(10, 6))

            try:
                plot_timeseries(model=model, exp=exp, source=source, variable=var,
                                resample=resample, plot_era5=plot_era5,
                                annual=annual, startdate=startdate,
                                enddate=enddate, std_startdate=std_startdate,
                                std_enddate=std_enddate,
                                monthly_std=monthly_std, annual_std=annual_std,
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

            if savefig:
                filename_pdf = create_filename(outputdir=outputdir,
                                               plotname=var, type="pdf",
                                               model=model, exp=exp,
                                               source=source)
                filename_pdf = os.path.join(outputdir_pdf, filename_pdf)
                logger.info(f"Output file: {filename_pdf}")
                fig.savefig(filename_pdf)

    if "timeseries_fomulae" in config:
        logger.info("Plotting timeseries formulae")

        for var in config["timeseries_fomulae"]:
            logger.info(f"Plotting {var}")

            # Creating the output filename
            filename_nc = create_filename(outputdir=outputdir,
                                          plotname=var, type="nc",
                                          model=model, exp=exp,
                                          source=source)
            filename_nc = os.path.join(outputdir_nc, filename_nc)
            logger.info(f"Output file: {filename_nc}")

            # Reading the configuration file
            plot_options = config["timeseries_plot_params"].get(var)
            if plot_options:
                plot_kw = plot_options.get("plot_kw", None)
                plot_era5 = plot_options.get("plot_era5", False)
                resample = plot_options.get("resample", "M")
                ylim = plot_options.get("ylim", {})
                reader_kw = plot_options.get("reader_kw", {})
                savefig = plot_options.get("savefig", True)
                annual = plot_options.get("annual", False)
                startdate = plot_options.get("startdate", None)
                enddate = plot_options.get("enddate", None)
                std_startdate = plot_options.get("std_startdate", "1991-01-01")
                std_enddate = plot_options.get("std_enddate", "2020-12-31")
                monthly_std = plot_options.get("monthly_std", False)
                annual_std = plot_options.get("annual_std", False)
            else:  # default
                plot_kw = config["timeseries_plot_params"]["default"].get("plot_kw", None)
                plot_era5 = config["timeseries_plot_params"]["default"].get("plot_era5", False)
                resample = config["timeseries_plot_params"]["default"].get("resample", "M")
                ylim = config["timeseries_plot_params"]["default"].get("ylim", {})
                reader_kw = config["timeseries_plot_params"]["default"].get("reader_kw", {})
                savefig = config["timeseries_plot_params"]["default"].get("savefig", True)
                annual = config["timeseries_plot_params"]["default"].get("annual", False)
                startdate = config["timeseries_plot_params"]["default"].get("startdate", None)
                enddate = config["timeseries_plot_params"]["default"].get("enddate", None)
                std_startdate = config["timeseries_plot_params"]["default"].get("std_startdate", "1991-01-01")
                std_enddate = config["timeseries_plot_params"]["default"].get("std_enddate", "2020-12-31")
                monthly_std = config["timeseries_plot_params"]["default"].get("monthly_std", False)
                annual_std = config["timeseries_plot_params"]["default"].get("annual_std", False)

            # Generating the image
            fig, ax = plt.subplots(figsize=(10, 6))

            try:
                plot_timeseries(model=model, exp=exp, source=source, variable=var,
                                resample=resample, plot_era5=plot_era5,
                                annual=annual, startdate=startdate,
                                enddate=enddate, std_startdate=std_startdate,
                                std_enddate=std_enddate,
                                monthly_std=monthly_std, annual_std=annual_std,
                                ylim=ylim, plot_kw=plot_kw, ax=ax,
                                reader_kw=reader_kw, outfile=filename_nc,
                                loglevel=loglevel, formula=True)
            except (NotEnoughDataError, NoDataError, NoObservationError) as e:
                logger.error(f"Error: {e}")
                continue
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error("This is a bug, please report it.")
                continue

            if savefig:
                filename_pdf = create_filename(outputdir=outputdir,
                                               plotname=var, type="pdf",
                                               model=model, exp=exp,
                                               source=source)
                filename_pdf = os.path.join(outputdir_pdf, filename_pdf)
                logger.info(f"Output file: {filename_pdf}")
                fig.savefig(filename_pdf)

    if "gregory" in config:
        logger.info("Plotting gregory plot")

        # Generating the image
        fig = plt.figure()

        try:
            plot_kw = config["gregory"]["plot_kw"]
        except KeyError:
            plot_kw = {}
        try:
            resample = config["gregory"]["resample"]
        except KeyError:
            logger.warning("No resample rate provided, using monthly.")
            resample = "M"
        try:
            reader_kw = config["gregory"]["reader_kw"]
        except KeyError:
            reader_kw = {}
        try:
            regrid = config["gregory"]["regrid"]
        except KeyError:
            logger.warning("No regrid provided, using raw data")
            regrid = None
        # Dictionary for Gregory plot
        try:
            ts = config["gregory"]["ts"]
        except KeyError:
            ts = '2t'
        try:
            toa = config["gregory"]["toa"]
        except KeyError:
            toa = ['mtnlwrf', 'mtnswrf']

        # Creating the output filename
        filename_nc = create_filename(outputdir=outputdir,
                                      plotname="gregory", type="nc",
                                      model=model, exp=exp,
                                      source=source, resample=resample)
        filename_nc = os.path.join(outputdir_nc, filename_nc)
        logger.info(f"Output file: {filename_nc}")

        try:
            fig = plot_gregory(model=model, exp=exp, source=source,
                               reader_kw=reader_kw, plot_kw=plot_kw,
                               outfile=filename_nc,
                               ts_name=ts, toa_name=toa,
                               regrid=regrid, freq=resample)
        except (NotEnoughDataError, NoDataError) as e:
            logger.error(f"Error: {e}")
        except Exception as e:
            logger.error(f"Error: {e}")
            logger.error("This is a bug, please report it.")

        if "savefig" in config["gregory"]:
            filename_pdf = create_filename(outputdir=outputdir,
                                           plotname="gregory", type="pdf",
                                           model=model, exp=exp,
                                           source=source, resample=resample)
            filename_pdf = os.path.join(outputdir_pdf, filename_pdf)
            logger.info(f"Output file: {filename_pdf}")
            fig.savefig(filename_pdf)

    logger.info("Analysis completed.")

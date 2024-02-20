#!/usr/bin/env python3
"""
Command-line interface for global time series diagnostic.

This CLI allows to plot timeseries of a set of variables
defined in a yaml configuration file for a single or multiple
experiments and gregory plot.
"""
import argparse
import os
import sys
import matplotlib.pyplot as plt

from aqua.util import load_yaml, get_arg
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

    # These will override the first one in the config file if provided
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser.parse_args(args)


def get_plot_options(config: dict = None,
                     var: str = None):
    """"
    Get plot options from the configuration file.
    Specific var options will override the default ones.

    Args:
        config: dictionary with the configuration file
        var: variable name

    Returns:
        plot options
    """
    plot_options = config["timeseries_plot_params"].get(var)
    if plot_options:
        monthly = plot_options.get("monthly", True)
        annual = plot_options.get("annual", True)
        regrid = plot_options.get("regrid", False)
        plot_ref = plot_options.get("plot_ref", True)
        plot_ref_kw = plot_options.get("plot_ref_kw", {'model': 'ERA5',
                                                       'exp': 'era5',
                                                       'source': 'monthly'})
        startdate = plot_options.get("startdate", None)
        enddate = plot_options.get("enddate", None)
        monthly_std = plot_options.get("monthly_std", True)
        annual_std = plot_options.get("annual_std", True)
        std_startdate = plot_options.get("std_startdate", None)
        std_enddate = plot_options.get("std_enddate", None)
        plot_kw = plot_options.get("plot_kw", {})
    else:
        monthly = config["timeseries_plot_params"]["default"].get("monthly", True)
        annual = config["timeseries_plot_params"]["default"].get("annual", True)
        regrid = config["timeseries_plot_params"]["default"].get("regrid", False)
        plot_ref = config["timeseries_plot_params"]["default"].get("plot_ref", True)
        plot_ref_kw = config["timeseries_plot_params"]["default"].get("plot_ref_kw", {'model': 'ERA5',
                                                                                      'exp': 'era5',
                                                                                      'source': 'monthly'})
        startdate = config["timeseries_plot_params"]["default"].get("startdate", None)
        enddate = config["timeseries_plot_params"]["default"].get("enddate", None)
        monthly_std = config["timeseries_plot_params"]["default"].get("monthly_std", True)
        annual_std = config["timeseries_plot_params"]["default"].get("annual_std", True)
        std_startdate = config["timeseries_plot_params"]["default"].get("std_startdate", None)
        std_enddate = config["timeseries_plot_params"]["default"].get("std_enddate", None)
        plot_kw = config["timeseries_plot_params"]["default"].get("plot_kw", {})
    return monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, enddate, \
        monthly_std, annual_std, std_startdate, std_enddate, plot_kw


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI Global Time Series')
    logger.info("Running Global Time Series diagnostic")

    # Moving to the current directory so that relative paths work
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f"Changing directory to {dname}")

    # Import diagnostic module
    sys.path.insert(0, "../../")
    from global_time_series import plot_gregory, Timeseries

    # Load configuration file
    file = get_arg(args, "config", "config_time_series_atm.yaml")
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    models = config['models']
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    logger.debug("Analyzing models:")
    models_list = []
    exp_list = []
    source_list = []

    for model in models:
        logger.debug(f"  - {model['model']} {model['exp']} {model['source']}")
        models_list.append(model['model'])
        exp_list.append(model['exp'])
        source_list.append(model['source'])

    outputdir = get_arg(args, "outputdir", config["outputdir"])

    if "timeseries" in config:
        logger.info("Plotting timeseries")

        for var in config["timeseries"]:
            logger.info(f"Plotting {var} time series")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw = get_plot_options(config, var)

            ts = Timeseries(var=var,
                            formula=False,
                            models=models_list,
                            exps=exp_list,
                            sources=source_list,
                            monthly=monthly,
                            annual=annual,
                            regrid=regrid,
                            plot_ref=plot_ref,
                            plot_ref_kw=plot_ref_kw,
                            startdate=startdate,
                            enddate=enddate,
                            monthly_std=monthly_std,
                            annual_std=annual_std,
                            std_startdate=std_startdate,
                            std_enddate=std_enddate,
                            plot_kw=plot_kw,
                            outdir=outputdir,
                            loglevel=loglevel)
            try:
                ts.run()
            except NotEnoughDataError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except NoDataError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except NoObservationError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except Exception as e:
                logger.error(f"Error plotting {var} time series: {e}")

    if "timeseries_formulae" in config:
        logger.info("Plotting timeseries formular")

        for var in config["timeseries_formulae"]:
            logger.info(f"Plotting {var} time series")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw = get_plot_options(config, var)

            ts = Timeseries(var=var,
                            formula=True,
                            models=models_list,
                            exps=exp_list,
                            sources=source_list,
                            monthly=monthly,
                            annual=annual,
                            regrid=regrid,
                            plot_ref=plot_ref,
                            plot_ref_kw=plot_ref_kw,
                            startdate=startdate,
                            enddate=enddate,
                            monthly_std=monthly_std,
                            annual_std=annual_std,
                            std_startdate=std_startdate,
                            std_enddate=std_enddate,
                            plot_kw=plot_kw,
                            outdir=outputdir,
                            loglevel=loglevel)
            try:
                ts.run()
            except NotEnoughDataError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except NoDataError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except NoObservationError as e:
                logger.warning(f"Skipping {var} time series plot: {e}")
            except Exception as e:
                logger.error(f"Error plotting {var} time series: {e}")

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
        ref = config["gregory"].get("ref", True)

        for model in models:
            try:
                fig = plot_gregory(model=model['model'],
                                   exp=model['exp'],
                                   source=model['source'],
                                   reader_kw=reader_kw,
                                   plot_kw=plot_kw,
                                   outfile=os.path.join(outputdir,
                                                        f"timeseries-gregory-{model['model']}-{model['exp']}.nc"),
                                   ref=ref, ts_name=ts, toa_name=toa,
                                   regrid=regrid, freq=resample)
            except (NotEnoughDataError, NoDataError) as e:
                logger.error(f"Error: {e}")
            except Exception as e:
                logger.error(f"Error: {e}")
                logger.error("This is a bug, please report it.")

        if "savefig" in config["gregory"]:
            filename_pdf = 'timeseries_gregory'
            for model in models:
                filename_pdf += f"_{model['model']}_{model['exp']}"
            filename_pdf += '.pdf'

            fig.savefig(os.path.join(outputdir, 'pdf', filename_pdf))

    logger.info("Analysis completed.")

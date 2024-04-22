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
from dask.distributed import Client, LocalCluster

from aqua.util import load_yaml, get_arg
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from global_time_series import Timeseries, GregoryPlot, SeasonalCycle


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Global time series CLI")

    parser.add_argument("-c", "--config",
                        type=str, required=False,
                        help="yaml configuration file")
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
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
        longname = plot_options.get("longname", None)
        expand = plot_options.get("expand", True)
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
        longname = None
        expand = config["timeseries_plot_params"]["default"].get("expand", True)
    return monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, enddate, \
        monthly_std, annual_std, std_startdate, std_enddate, plot_kw, longname, expand


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

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

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
                plot_kw, longname, expand = get_plot_options(config, var)

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
                            longname=longname,
                            expand=expand,
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
                plot_kw, longname, expand = get_plot_options(config, var)

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
                            longname=longname,
                            expand=expand,
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

        config_gregory = config["gregory"]

        ts_name = config_gregory.get("ts", "2t")
        toa_name = config_gregory.get("toa", ["mtnlwrf", "mtnswrf"])
        monthly = config_gregory.get("monthly", True)
        annual = config_gregory.get("annual", True)
        ref = config_gregory.get("ref", True)
        regrid = config_gregory.get("regrid", None)
        ts_std_start = config_gregory.get("ts_std_start", "1980-01-01")
        ts_std_end = config_gregory.get("ts_std_end", "2010-12-31")
        toa_std_start = config_gregory.get("toa_std_start", "2001-01-01")
        toa_std_end = config_gregory.get("toa_std_end", "2020-12-31")

        gp = GregoryPlot(models=models_list,
                         exps=exp_list,
                         sources=source_list,
                         monthly=monthly,
                         annual=annual,
                         ref=ref,
                         ts_name=ts_name,
                         toa_name=toa_name,
                         ts_std_start=ts_std_start,
                         ts_std_end=ts_std_end,
                         toa_std_start=toa_std_start,
                         toa_std_end=toa_std_end,
                         outdir=outputdir,
                         regrid=regrid,
                         loglevel=loglevel)

        try:
            gp.run()
        except NotEnoughDataError as e:
            logger.warning(f"Skipping gregory plot: {e}")
        except NoDataError as e:
            logger.warning(f"Skipping gregory plot: {e}")
        except NoObservationError as e:
            logger.warning(f"Skipping gregory plot: {e}")
        except Exception as e:
            logger.error(f"Error plotting gregory plot: {e}")

    if "seasonal_cycle" in config:
        logger.info("Plotting seasonal cycle")

        for var in config["seasonal_cycle"]:
            logger.info(f"Plotting {var} seasonal cycle")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw, longname, _ = get_plot_options(config, var)

            sc = SeasonalCycle(var=var,
                               formula=False,
                               models=models_list,
                               exps=exp_list,
                               sources=source_list,
                               regrid=regrid,
                               plot_ref=plot_ref,
                               plot_ref_kw=plot_ref_kw,
                               startdate=startdate,
                               enddate=enddate,
                               std_startdate=std_startdate,
                               std_enddate=std_enddate,
                               plot_kw=plot_kw,
                               outdir=outputdir,
                               loglevel=loglevel)
            try:
                sc.run()
            except NotEnoughDataError as e:
                logger.warning(f"Skipping {var} seasonal cycle plot: {e}")
            except NoDataError as e:
                logger.warning(f"Skipping {var} seasonal cycle plot: {e}")
            except NoObservationError as e:
                logger.warning(f"Skipping {var} seasonal cycle plot: {e}")
            except Exception as e:
                logger.error(f"Error plotting {var} seasonal cycle: {e}")
    logger.info("Global Time Series is terminated.")

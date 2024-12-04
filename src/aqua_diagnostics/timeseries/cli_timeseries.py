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

from aqua.util import load_yaml, get_arg, ConfigPath
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua.diagnostics.timeseries import Timeseries, GregoryPlot, SeasonalCycle


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
    parser.add_argument("--catalog", type=str,
                        required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    # Adding new output configuration options
    parser.add_argument("--rebuild", action="store_true",
                        help="Whether to rebuild the plots")
    parser.add_argument("--no_rebuild", action="store_true",
                        help="Disable rebuilding of plots")
    parser.add_argument("--save_netcdf", action="store_true",
                        help="Whether to save plots as NetCDF")
    parser.add_argument("--no_save_netcdf", action="store_true",
                        help="Disable saving plots as NetCDF")
    parser.add_argument("--save_pdf", action="store_true",
                        help="Whether to save plots as PDF")
    parser.add_argument("--no_save_pdf", action="store_true",
                        help="Disable saving plots as PDF")
    parser.add_argument("--save_png", action="store_true",
                        help="Whether to save plots as PNG")
    parser.add_argument("--no_save_png", action="store_true",
                        help="Disable saving plots as PNG")
    parser.add_argument("--dpi", type=int, required=False,
                        help="DPI for saved images")
    parser.add_argument("--filename_keys", type=str, nargs='*',
                        help="Keys to use for filenames")

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
        units = plot_options.get("units", None)
        extend = plot_options.get("extend", True)
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
        units = None
        extend = config["timeseries_plot_params"]["default"].get("extend", True)
    return monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, enddate, \
        monthly_std, annual_std, std_startdate, std_enddate, plot_kw, longname, units, extend


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI Timeseries')
    logger.info("Running Timeseries diagnostic")

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    if nworkers:
        cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
        client = Client(cluster)
        logger.info(f"Running with {nworkers} dask distributed workers.")

    # Load configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "timeseries",
                                  "config_timeseries_atm.yaml")
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    # Override the first model in the config file if provided in the command line
    models = config['models']
    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    # Override output parameters if provided as command-line arguments
    outputdir = get_arg(args, "outputdir", config['output'].get("outputdir"))
    rebuild = not args.no_rebuild if args.rebuild or args.no_rebuild else config['output'].get("rebuild")
    save_netcdf = not args.no_save_netcdf if args.save_netcdf or args.no_save_netcdf else config['output'].get("save_netcdf")
    save_pdf = not args.no_save_pdf if args.save_pdf or args.no_save_pdf else config['output'].get("save_pdf")
    save_png = not args.no_save_png if args.save_png or args.no_save_png else config['output'].get("save_png")
    dpi = args.dpi if args.dpi is not None else config['output'].get("dpi")
    filename_keys = args.filename_keys if args.filename_keys else config['output'].get("filename_keys", [])

    logger.debug(f"Saving settings - NetCDF: {'enabled' if save_netcdf else 'disabled'}, PDF: {'enabled' if save_pdf else 'disabled'}, PNG: {'enabled' if save_png else 'disabled'}")

    # Prepare model-related lists for the diagnostics
    logger.debug("Analyzing models:")
    catalogs_list = []
    models_list = []
    exp_list = []
    source_list = []

    for model in models:
        logger.debug(f"  - {model['catalog']} {model['model']} {model['exp']} {model['source']}")
        catalogs_list.append(model['catalog'])
        models_list.append(model['model'])
        exp_list.append(model['exp'])
        source_list.append(model['source'])

    if "timeseries" in config:
        logger.info("Plotting timeseries")

        for var in config["timeseries"]:
            logger.info(f"Plotting {var} timeseries")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw, longname, units, extend = get_plot_options(config, var)

            ts = Timeseries(var=var,
                            formula=False,
                            catalogs=catalogs_list,
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
                            units=units,
                            extend=extend,
                            plot_kw=plot_kw,
                            outdir=outputdir,
                            loglevel=loglevel,
                            rebuild=rebuild,
                            filename_keys=filename_keys,
                            save_netcdf=save_netcdf,
                            save_pdf=save_pdf,
                            save_png=save_png,
                            dpi=dpi)
            try:
                ts.run()
            except NotEnoughDataError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except NoDataError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except NoObservationError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except Exception as e:
                logger.error(f"Error plotting {var} timeseries: {e}")

    if "timeseries_formulae" in config:
        logger.info("Plotting timeseries formula")

        for var in config["timeseries_formulae"]:
            logger.info(f"Plotting {var} timeseries")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw, longname, units, extend = get_plot_options(config, var)

            ts = Timeseries(var=var,
                            formula=True,
                            catalogs=catalogs_list,
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
                            units=units,
                            extend=extend,
                            outdir=outputdir,
                            loglevel=loglevel,
                            rebuild=rebuild,
                            filename_keys=filename_keys,
                            save_netcdf=save_netcdf,
                            save_pdf=save_pdf,
                            save_png=save_png,
                            dpi=dpi)
            try:
                ts.run()
            except NotEnoughDataError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except NoDataError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except NoObservationError as e:
                logger.warning(f"Skipping {var} timeseries plot: {e}")
            except Exception as e:
                logger.error(f"Error plotting {var} timeseries: {e}")

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

        gp = GregoryPlot(catalogs=catalogs_list,
                         models=models_list,
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
                         loglevel=loglevel,
                         rebuild=rebuild,
                         filename_keys=filename_keys,
                         save_netcdf=save_netcdf,
                         save_pdf=save_pdf,
                         save_png=save_png,
                         dpi=dpi)

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
                plot_kw, longname, units, _ = get_plot_options(config, var)

            sc = SeasonalCycle(var=var,
                               formula=False,
                               catalogs=catalogs_list,
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
                               longname=longname,
                               units=units,
                               loglevel=loglevel,
                               rebuild=rebuild,
                               filename_keys=filename_keys,
                               save_netcdf=save_netcdf,
                               save_pdf=save_pdf,
                               save_png=save_png,
                               dpi=dpi)
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

    if "seasonal_cycle_formulae" in config:
        logger.info("Plotting seasonal cycle formulae")

        for var in config["seasonal_cycle_formulae"]:
            logger.info(f"Plotting {var} seasonal cycle")
            monthly, annual, regrid, plot_ref, plot_ref_kw, startdate, \
                enddate, monthly_std, annual_std, std_startdate, std_enddate, \
                plot_kw, longname, units, _ = get_plot_options(config, var)

            sc = SeasonalCycle(var=var,
                               formula=True,
                               catalogs=catalogs_list,
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
                               longname=longname,
                               units=units,
                               loglevel=loglevel,
                               rebuild=rebuild,
                               filename_keys=filename_keys,
                               save_netcdf=save_netcdf,
                               save_pdf=save_pdf,
                               save_png=save_png,
                               dpi=dpi)
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

    logger.info("Timeseries is terminated.")

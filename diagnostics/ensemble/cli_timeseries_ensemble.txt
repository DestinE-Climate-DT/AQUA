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
#from global_time_series import Timeseries, GregoryPlot, SeasonalCycle

from timeseries_ensemble import Ensemble_timeseries 


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
    #parser.add_argument("--catalog", type=str,
    #                    required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser.parse_args(args)

def get_plot_options(config: dict = None, var: str = None):
    plot_options = config["timeseries_plot_params"].get(var)
    #regrid = plot_options.get("regrid", False)
    startdate = config["timeseries_plot_params"]["default"].get("startdate", None)
    enddate = config["timeseries_plot_params"]["default"].get("enddate", None)
    plot_kw = config["timeseries_plot_params"]["default"].get("plot_kw", {})
    units = None
    return startdate,enddate,plot_kw,units

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
    file = get_arg(args, "config", "config_timeseries_ensemble.yaml")
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    models = config['models']
    #models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    logger.debug("Analyzing models:")
    #catalogs_list = []
    models_list = []
    exps_list = []
    sources_list = []

    for model in models:
        #logger.debug(f"  - {model['catalog']} {model['model']} {model['exp']} {model['source']}")
        #catalogs_list.append(model['catalog'])
        models_list.append(model['model'])
        exps_list.append(model['exp'])
        sources_list.append(model['source'])

    outputdir = get_arg(args, "outputdir", config["outputdir"])

    if "timeseries" in config:
        var = config['timeseries']
        logger.info(f"Plotting {var} timeseries")
        startdate,enddate,plot_kw,units = get_plot_options(config,var)
        #print("here!!!!!! ",startdate, var)

        #ts = Ensemble_timeseries(var=var,catalogs=catalogs_list,models=models_list,exps=exps_list,sources=sources_list,startdate=startdate,enddate=enddate,loglevel=loglevel)
        ts = Ensemble_timeseries(var=var,models=models_list,exps=exps_list,sources=sources_list,startdate=startdate,enddate=enddate,loglevel=loglevel)
        
        try:
            ts.run()
        except Exception as e:
            logger.error(f'Error plotting {var} timeseries: {e}')


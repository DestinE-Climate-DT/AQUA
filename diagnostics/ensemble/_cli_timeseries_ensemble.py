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
    mon_startdate = config["timeseries_plot_params"].get("monthly_startdate", None)
    mon_enddate = config["timeseries_plot_params"].get("monthly_enddate", None)
    ann_startdate = config["timeseries_plot_params"].get("annual_startdate",None)
    ann_enddate = config["timeseries_plot_params"].get("annual_enddate",None)
    plot_kw = config["timeseries_plot_params"].get("plot_kw", {})
    units = None
    return mon_startdate,mon_enddate,ann_startdate,ann_enddate,plot_kw,units

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

    mon_model = config['models_monthly']
    mon_model_list = [] 
    mon_exp_list = [] 
    mon_source_list = []
    mon_model[0]['model'] = get_arg(args, 'model', mon_model[0]['model'])
    mon_model[0]['exp'] = get_arg(args,'exp',mon_model[0]['exp'])
    mon_model[0]['source'] = get_arg(args,'source',mon_model[0]['source'])
    for model in mon_model:
        mon_model_list.append(model['model'])
        mon_exp_list.append(model['exp'])
        mon_source_list.append(model['source'])        

    ann_model = config['models_annual']
    ann_model_list = [] 
    ann_exp_list = [] 
    ann_source_list = []
    ann_model[0]['model'] = get_arg(args, 'model',ann_model[0]['model'])
    ann_model[0]['exp'] = get_arg(args,'exp',ann_model[0]['exp'])
    ann_model[0]['source'] = get_arg(args,'source',ann_model[0]['source'])
    for model in ann_model:
        ann_model_list.append(model['model'])
        ann_exp_list.append(model['exp'])
        ann_source_list.append(model['source'])        

    ref_mon = config['reference_model_monthly']
    ref_mon_dict = {'models':ref_mon[0]['model'],'exps':ref_mon[0]['exp'],'sources':ref_mon[0]['source']}
    
    ref_ann = config['reference_model_annual']
    ref_ann_dict = {'models':ref_ann[0]['model'],'exps':ref_ann[0]['exp'],'sources':ref_ann[0]['source']}
    
    logger.debug("Analyzing models:")
    
    outputdir = get_arg(args, "outputdir", config["outputdir"])
    
    if "timeseries" in config:
        var = config['timeseries']
        logger.info(f"Plotting {var} timeseries")
        mon_startdate,mon_enddate,ann_startdate,ann_enddate,plot_kw,units = get_plot_options(config,var)

        ts = Ensemble_timeseries(var=var,mon_model=mon_model_list,mon_exp=mon_exp_list,mon_source=mon_source_list,ann_model=ann_model_list,ann_exp=ann_exp_list,ann_source=ann_source_list,ref_mon_dict=ref_mon_dict,ref_ann_dict=ref_ann_dict,mon_startdate=mon_startdate,mon_enddate=mon_enddate,ann_startdate=ann_startdate,ann_enddate=ann_enddate,plot_kw=plot_kw,outdir=outputdir,loglevel=loglevel)
        try:
            ts.run()
        except Exception as e:
            logger.error(f'Error plotting {var} timeseries: {e}')


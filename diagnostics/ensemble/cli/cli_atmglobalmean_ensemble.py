#!/usr/bin/env python3
"""
Command-line interface for ensemble atmglobalmean diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import os
import sys
from dask.distributed import Client, LocalCluster

from aqua.util import load_yaml, get_arg
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure

script_dir = os.path.dirname(os.path.abspath(__file__))
ensemble_module_path = os.path.join(script_dir, "../../")
sys.path.insert(0, ensemble_module_path)

from ensemble import EnsembleLatLon


def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Ensemble atmglobalmean map CLI")

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
    figure_size = config["atmglobalmean_plot_params"].get("figure_size",None)
    plot_std = config["atmglobalmean_plot_params"].get("plot_std",None)
    plot_label = config["atmglobalmean_plot_params"].get("plot_label",None)
    #label_ncol = config["atmglobalmean_plot_params"].get("label_ncol",None)
    label_size = config["atmglobalmean_plot_params"].get("label_size",None)
    pdf_save = config["atmglobalmean_plot_params"].get("pdf_save",None)
    #units = config["atmglobalmean_plot_params"].get("units",None)
    return figure_size,plot_std,plot_label,label_size,pdf_save

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI multi-model ensemble calculation of atmglobalmean')
    logger.info("Running multi-model ensemble calculation of atmglobalmean")

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
    file = get_arg(args, "config", "config_atmglobalmean_ensemble.yaml")
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    model = config['models']
    model_list = [] 
    exp_list = [] 
    source_list = []
    if model != None:
        model[0]['model'] = get_arg(args, 'model', model[0]['model'])
        model[0]['exp'] = get_arg(args,'exp',model[0]['exp'])
        model[0]['source'] = get_arg(args,'source',model[0]['source'])
        for model in model:
            model_list.append(model['model'])
            exp_list.append(model['exp'])
            source_list.append(model['source'])
    var = config['atmglobalmean']
    logger.info(f"Plotting {var} atmglobalmean map")
    figure_size,plot_std,plot_label,label_size,pdf_save = get_plot_options(config,var)

    logger.debug("Analyzing models:")
    
    outdir = get_arg(args, "outputdir", config["outputdir"])
    outfile = 'aqua-analysis-ensemble-atmglobalmean-map' 
    atmglobalmean_ens = EnsembleLatLon(var=var,model=model_list,exp=exp_list,source=source_list,plot_std=plot_std,figure_size=figure_size,plot_label=plot_label,label_size=label_size,outdir=outdir,outfile=outfile,pdf_save=pdf_save,loglevel=loglevel)
    try:
        atmglobalmean_ens.run()
    except Exception as e:
        logger.error(f'Error plotting {var} ensemble: {e}')
    

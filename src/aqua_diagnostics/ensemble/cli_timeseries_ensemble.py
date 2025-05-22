#!/usr/bin/env python3
"""
Command-line interface for ensemble global time series diagnostic.

This CLI allows to plot ensemle of global timeseries of a variable
defined in a yaml configuration file for multiple models.
"""
import argparse
import os
import sys
import gc
import xarray as xr
from dask.distributed import Client, LocalCluster
import numpy as np
import pandas as pd
from aqua.util import load_yaml, get_arg, ConfigPath
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua import Reader
from aqua.diagnostics import EnsembleTimeseries

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
    # parser.add_argument("--catalog", type=str,
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


def get_plot_options(config: dict = None, variable: str = None):
    """
    Extracts timeseries plot options from a configuration dictionary.

    This function retrieves a set of parameters related to timeseries plotting from the 
    `timeseries_plot_params` key of the provided config file.

    Args:
        config (config file): Settings are defined in the config file 
            which is load by the load_yaml function. 
            It is expected to include the key `timeseries_plot_params` with 
            sub-keys for various plotting parameters. Defaults to None.
        variable (str): A variable name (not used in the current implementation, 
            but reserved for future use). Defaults to None.
            
    Returns:
        tuple: A tuple containing the following elements extracted from the 
        `timeseries_plot_params` key in the configuration:
            - startdate (any): The start date for the timeseries plot (default: None if not found).
            - enddate (any): The end date for the timeseries plot (default: None).
            - plot_std (any): Flag or settings for plotting standard deviations (default: None).
            - plot_ensemble_members (any): Flag or settings for plotting ensemble members (default: None).
            - ensemble_label (any): Label for ensemble data (default: None).
            - figure_size (any): Size of the figure (default: None).
            - ref_label (any): Label for the reference data (default: None).
            - label_ncol (any): Number of columns for the plot legend labels (default: None).
            - label_size (any): Font size of the labels (default: None).
            - pdf_save (any): Whether to save the plot as a PDF (default: None).
            - units (any): Units for the data being plotted (default: None).
    """
    startdate = config["timeseries_plot_params"].get("startdate", None)
    enddate = config["timeseries_plot_params"].get("enddate", None)
    plot_std = config["timeseries_plot_params"].get("plot_std", None)
    plot_ensemble_members = config["timeseries_plot_params"].get("plot_ensemble_members", None)
    ensemble_label = config["timeseries_plot_params"].get("ensemble_label", None)
    figure_size = config["timeseries_plot_params"].get("figure_size", None)
    ref_label = config["timeseries_plot_params"].get("ref_label", None)
    label_ncol = config["timeseries_plot_params"].get("label_ncol", None)
    label_size = config["timeseries_plot_params"].get("label_size", None)
    pdf_save = config["timeseries_plot_params"].get("pdf_save", None)
    units = config["timeseries_plot_params"].get("units", None)
    return startdate, enddate, plot_std, plot_ensemble_members, ensemble_label, figure_size, ref_label, label_ncol, label_size, pdf_save, units


def retrieve_data(variable=None, models=None, exps=None, sources=None, startdate=None, enddate=None, ens_dim="Ensembles"):
    """
    Retrieves, merges, and slices datasets based on specified models, experiments, 
    sources, and time boundaries.

    This function reads data for a given variable (`variable`) from multiple models, experiments, 
    and sources, combines the datasets along the specified ensemble dimension, and slices 
    the merged dataset to the given start and end dates.

    Args:
        variable (str): The variable to retrieve data for. Defaults to None.
        models (list): A list of model names. Each model corresponds to an 
            experiment and source in the `exps` and `sources` lists, respectively. 
            Defaults to None.
        exps (list): A list of experiment names. Each experiment corresponds 
            to a model and source in the `models` and `sources` lists, respectively. 
            Defaults to None.
        sources (list): A list of data source names. Each source corresponds 
            to a model and experiment in the `models` and `exps` lists, respectively. 
            Defaults to None.
        startdate (str or datetime): The start date for slicing the merged dataset. 
            If None, the maximum start date across all datasets is used. Defaults to None.
        enddate (str or datetime): The end date for slicing the merged dataset. 
            If None, the minimum end date across all datasets is used. Defaults to None.
        ens_dim (str, optional): The name of the dimension along which the datasets are 
            concatenated. Defaults to "Ensembles".

    Returns:
        tuple:
            - startdate (datetime): The resolved start date for the sliced dataset.
            - enddate (datetime): The resolved end date for the sliced dataset.
            - xarray.Dataset: The merged dataset containing data from all specified models, 
              experiments, and sources, concatenated along `ens_dim` and sliced 
              to the time range `[startdate, enddate]`.
    """
    dataset_list = []
    startdate_list = []
    enddate_list = []
    if models is None or exps is None or sources is None:
        raise NoDataError("No models, exps or sources provided")
    else:
        for i, model in enumerate(models):
            reader = Reader(model=model, exp=exps[i], source=sources[i], areas=False, startdate=startdate, enddate=enddate)
            data = reader.retrieve(var=variable)
            dataset_list.append(data)
            startdate_list.append(data.time[0].values)
            enddate_list.append(data.time[-1].values)
    merged_dataset = xr.concat(dataset_list, ens_dim)
    
    if startdate != None:
        istartdate = max(startdate_list)
        startdate = pd.to_datetime(startdate) 
        startdate = max(istartdate, startdate)
    if enddate != None:
        ienddate = min(enddate_list)
        enddate = pd.to_datetime(enddate)
        enddate = min(ienddate, enddate)
    if startdate == None:        
        startdate = max(startdate_list)
    if enddate == None:
        enddate = min(enddate_list)

    merged_dataset = merged_dataset.sel(time=slice(startdate, enddate))
    del reader
    del data
    del dataset_list
    del startdate_list
    del enddate_list
    gc.collect()
    return startdate, enddate, merged_dataset


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI multi-model Timeseries ensemble')
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
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "ensemble",
                                  "config_timeseries_ensemble.yaml")
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)
    #file = get_arg(args, "config", "config_timeseries_ensemble.yaml")
    #logger.info(f"Reading configuration file {file}")
    #config = load_yaml(file)

    variable = config['variable']
    logger.info(f"Plotting {variable} timeseries")
    startdate, enddate, plot_std, plot_ensemble_members, ensemble_label, figure_size, ref_label, label_ncol, label_size, pdf_save, units = get_plot_options(
        config, variable)

    outputdir = get_arg(args, "outputdir", config["outputdir"])

    # Monthly model data
    mon_model = config['models_monthly']
    mon_model_list = []
    mon_exp_list = []
    mon_source_list = []
    if mon_model != None:
        mon_model[0]['model'] = get_arg(args, 'model', mon_model[0]['model'])
        mon_model[0]['exp'] = get_arg(args, 'exp', mon_model[0]['exp'])
        mon_model[0]['source'] = get_arg(args, 'source', mon_model[0]['source'])
        for model in mon_model:
            mon_model_list.append(model['model'])
            mon_exp_list.append(model['exp'])
            mon_source_list.append(model['source'])

    mon_startdate, mon_enddate, mon_dataset = retrieve_data(
        variable, models=mon_model_list, exps=mon_exp_list, sources=mon_source_list, startdate=startdate, enddate=enddate)

    # Annual model data
    ann_model = config['models_annual']
    ann_model_list = []
    ann_exp_list = []
    ann_source_list = []
    if ann_model != None:
        ann_model[0]['model'] = get_arg(args, 'model', ann_model[0]['model'])
        ann_model[0]['exp'] = get_arg(args, 'exp', ann_model[0]['exp'])
        ann_model[0]['source'] = get_arg(args, 'source', ann_model[0]['source'])
        for model in ann_model:
            ann_model_list.append(model['model'])
            ann_exp_list.append(model['exp'])
            ann_source_list.append(model['source'])

    ann_startdate, ann_enddate, ann_dataset = retrieve_data(
        variable, models=mon_model_list, exps=mon_exp_list, sources=ann_source_list, startdate=startdate, enddate=enddate)

    # Reference monthly data
    ref_mon = config['reference_model_monthly']
    if ref_mon != None:
        ref_mon_model = ref_mon[0]['model']
        ref_mon_exp = ref_mon[0]['exp']
        ref_mon_source = ref_mon[0]['source']

    reader = Reader(model=ref_mon_model, exp=ref_mon_exp, source=ref_mon_source,
                    startdate=mon_startdate, enddate=mon_enddate, areas=False,variable=variable)
    ref_mon_dataset = reader.retrieve(var=variable)

    # Reference annual data
    ref_ann = config['reference_model_annual']
    if ref_ann != None:
        ref_ann_model = ref_ann[0]['model']
        ref_ann_exp = ref_ann[0]['exp']
        ref_ann_source = ref_ann[0]['source']

    reader = Reader(model=ref_ann_model, exp=ref_ann_exp, source=ref_ann_source,
                    startdate=ann_startdate, enddate=ann_enddate, areas=False,variable=variable)
    ref_ann_dataset = reader.retrieve(var=variable)

    ts = EnsembleTimeseries(var=variable, mon_model_dataset=mon_dataset, ann_model_dataset=ann_dataset,
                            mon_ref_data=ref_mon_dataset, ann_ref_data=ref_ann_dataset)
    try:
        ts.edit_attributes(plot_std=plot_std, plot_ensemble_members=plot_ensemble_members, ensemble_label=ensemble_label,
                         ref_label=ref_label, figure_size=figure_size, label_ncol=label_ncol, label_size=label_size, pdf_save=pdf_save)
        ts.run()
    except Exception as e:
        logger.error(f'Error plotting {variable} timeseries: {e}')

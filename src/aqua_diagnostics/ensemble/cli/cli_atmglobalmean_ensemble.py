#!/usr/bin/env python3
"""
Command-line interface for ensemble atmglobalmean diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import os
import sys
import gc
import xarray as xr
from dask.distributed import Client, LocalCluster
from aqua import Reader
from aqua.util import load_yaml, get_arg
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure

from aqua.diagnostics import EnsembleLatLon

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
    Extracts plotting options from the given config file.

    This function retrieves a set of parameters related to plotting from the 
    `atmglobalmean_plot_params` key of the provided config file.

    Args:
        config (config file): Settings are defined in the config file 
            which is load by the load_yaml function. 
            It is expected to include the key `atmglobalmean_plot_params` with 
            sub-keys for various plotting parameters. Defaults to None.
        variable (str): A variable name (not used in the current implementation, 
            but reserved for future use). Defaults to None.

    Returns:
        tuple: A tuple containing the following elements extracted from the 
        `atmglobalmean_plot_params` key in the configuration:
            - figure_size (any): The size of the figure (default: None if not found).
            - plot_std (any): Standard deviation plotting flag or parameters (default: None).
            - plot_label (any): The label for the plot (default: None).
            - pdf_save (any): Whether to save the plot as a PDF (default: None).
            - units (any): Units for the data being plotted (default: None).
            - mean_plot_title (any): Title for the mean plot (default: None).
            - std_plot_title (any): Title for the standard deviation plot (default: None).
            - cbar_label (any): Label for the color bar (default: None).

    Note:
        If `config` or `atmglobalmean_plot_params` is not provided or incomplete, 
        the returned tuple will contain `None` for the missing parameters.
    """
    figure_size = config["atmglobalmean_plot_params"].get("figure_size", None)
    plot_std = config["atmglobalmean_plot_params"].get("plot_std", None)
    plot_label = config["atmglobalmean_plot_params"].get("plot_label", None)
    pdf_save = config["atmglobalmean_plot_params"].get("pdf_save", None)
    units = config["atmglobalmean_plot_params"].get("units", None)
    mean_plot_title = config["atmglobalmean_plot_params"].get("mean_plot_title", None)
    std_plot_title = config["atmglobalmean_plot_params"].get("std_plot_title", None)
    cbar_label = config["atmglobalmean_plot_params"].get("cbar_label", None)
    return figure_size, plot_std, plot_label, pdf_save, units, mean_plot_title, std_plot_title, cbar_label


def retrieve_data(variable=None, models=None, exps=None, sources=None, ens_dim="Ensembles"):
    """
    Retrieves and merges datasets based on specified models, experiments, and sources.

    This function reads data for a given variable (` variable`) from multiple models, experiments, 
    and sources, combines them along the specified ensemble dimension, and returns the 
    merged dataset.

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
        ens_dim (str, optional): The name of the dimension along which the datasets 
            are concatenated. Defaults to "Ensembles".

    Returns:
        xarray.Dataset: A merged dataset containing data from all specified models, 
        experiments, and sources, concatenated along the `ens_dim` dimension.
    """
    dataset_list = []
    if models is None or exps is None or sources is None:
        raise NoDataError("No models, exps or sources provided")
    else:
        for i, model in enumerate(models):
            reader = Reader(model=model, exp=exps[i], source=sources[i], areas=False, variable=variable)
            data = reader.retrieve(var=variable)
            dataset_list.append(data)
    merged_dataset = xr.concat(dataset_list, ens_dim)
    del reader
    del data
    del dataset_list
    gc.collect()
    return merged_dataset


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

    variable = config['atmglobalmean']
    logger.info(f"Plotting {variable} atmglobalmean map")
    figure_size, plot_std, plot_label, pdf_save, units, mean_plot_title, std_plot_title, cbar_label = get_plot_options(
        config,  variable)

    model = config['models']
    model_list = []
    exp_list = []
    source_list = []
    if model != None:
        model[0]['model'] = get_arg(args, 'model', model[0]['model'])
        model[0]['exp'] = get_arg(args, 'exp', model[0]['exp'])
        model[0]['source'] = get_arg(args, 'source', model[0]['source'])
        for model in model:
            model_list.append(model['model'])
            exp_list.append(model['exp'])
            source_list.append(model['source'])

    atm_dataset = retrieve_data(variable, models=model_list, exps=exp_list, sources=source_list)

    outdir = get_arg(args, "outputdir", config["outputdir"])
    outfile = 'aqua-analysis-ensemble-atmglobalmean-map'
    atmglobalmean_ens = EnsembleLatLon(var= variable, dataset=atm_dataset)
    try:
        atmglobalmean_ens.edit_attributes(cbar_label=cbar_label)  # to change class attributes
        atmglobalmean_ens.run()

    except Exception as e:
        logger.error(f'Error plotting {variable} ensemble: {e}')

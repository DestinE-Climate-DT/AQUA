# Imports for CLI
import sys
import os
import argparse
from dask.distributed import Client, LocalCluster
import numpy as np
from aqua.util import load_yaml, get_arg, OutputSaver, create_folder
from aqua import Reader
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua.diagnostics import Radiation

def parse_arguments(args):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Radiation CLI')
    parser.add_argument('-c', '--config', type=str, help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int, help='number of Dask distributed workers')
    parser.add_argument("--loglevel", "-l", type=str, required=False, help="loglevel")                  
    # Override config file if provided
    parser.add_argument("--catalog", type=str, required=False, help="catalog name")
    parser.add_argument('--model', type=str, help='model name', required=False)
    parser.add_argument('--exp', type=str, help='experiment name', required=False)
    parser.add_argument('--source', type=str, help='source name', required=False)
    parser.add_argument('--outputdir', type=str, help='output directory', required=False)
    return parser.parse_args(args)

def initialize_dask(nworkers, logger):
    """Set up a Dask distributed cluster with a specified number of workers."""
    cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
    client = Client(cluster)
    logger.info(f"Running with {nworkers} Dask distributed workers.")
    return client

if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI Radiation')
    logger.info("Running Radiation diagnostic")

    # Move to script directory
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        logger.info(f"Changing directory to {dname}")

    # Initialize Dask if nworkers provided
    nworkers = get_arg(args, 'nworkers', None)
    client = None
    if nworkers:
        client = initialize_dask(nworkers, logger)

    homedir = os.environ.get('HOME')
    config_filename = os.path.join(homedir, '.aqua', 'diagnostics', 'radiation', 'cli', 'config_radiation-boxplots.yaml')

    # Load the configuration
    file = get_arg(args, "config", config_filename)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    # Process model information and datasets
    models = config['models']
    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])

    logger.debug("Analyzing models:")
    models_list, exp_list, datasets = [], [], []
    variables = config['diagnostic_attributes'].get('variables', ['-tnlwrf', 'tnswrf'])

    for model in models:
        try:
            reader = Reader(catalog=model['catalog'], model=model['model'], exp=model['exp'], source=model['source'])
            dataset = reader.retrieve()
        except Exception as e:
            logger.error(f"No model data found: {e}")
            logger.critical("Radiation diagnostic is terminated.")
            sys.exit(0)
        datasets.append(dataset)
        models_list.append(model['model'])
        exp_list.append(model['exp'])

    # Create output directory
    outputdir = get_arg(args, "outputdir", config['output'].get("outputdir"))
    rebuild = config['output'].get("rebuild")
    filename_keys = config['output'].get("filename_keys")
    save_netcdf = config['output'].get("save_netcdf")
    save_pdf = config['output'].get("save_pdf")
    save_png = config['output'].get("save_png")
    dpi = config['output'].get("dpi")

    # Output naming and saving
    output_saver = OutputSaver(diagnostic='radiation', exp=exp_list[0], model=models_list[0], loglevel=loglevel,
                               default_path=outputdir, rebuild=rebuild, filename_keys=filename_keys)
    logger.info("Boxplot generation")
    radiation = Radiation()
    result = radiation.boxplot(datasets=datasets, model_names=models_list, variables=variables)

    description = (
        f"Boxplot of radiation variables ({', '.join(variables)}) for the period defined in the configuration file. "
        f"The analysis includes the {models_list[0]} model (experiment {exp_list[0]}) from the provided catalog, "
        f"alongside other models defined in the configuration ({', '.join(models_list[1:])} if applicable). "
        f"This boxplot provides a comparison of radiation fluxes across the models, allowing an evaluation of "
        f"the variability and bias among different datasets. "
    )
    metadata = {"Description": description}
    if result:
        fig, ax, netcdf = result
        if save_netcdf:
            output_saver.save_netcdf(dataset=netcdf, diagnostic_product='boxplot', metadata=metadata)
        if save_pdf:
            output_saver.save_pdf(fig=fig, diagnostic_product='boxplot', metadata=metadata, dpi=dpi)
        if save_png:
            output_saver.save_png(fig=fig, diagnostic_product='boxplot', metadata=metadata, dpi=dpi)

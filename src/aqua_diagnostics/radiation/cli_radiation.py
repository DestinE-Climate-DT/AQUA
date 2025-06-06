# Imports for CLI
import argparse
import os
import sys
import pandas as pd

from dask.distributed import Client, LocalCluster

from aqua import Reader
from aqua.util import load_yaml, get_arg, ConfigPath, OutputSaver
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua.diagnostics import Radiation

def parse_arguments(args):
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description="Radiation CLI")

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
    parser.add_argument("--regrid", type=str,
                        required=False, help="target regrid resolution")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")
    parser.add_argument("--cluster", type=str,
                        required=False, help="dask cluster address")
    return parser.parse_args(args)

if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI Radiation')
    logger.info("Running Radiation diagnostic")

    # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    cluster = get_arg(args, 'cluster', None)
    private_cluster = False
    if nworkers or cluster:
        if not cluster:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            logger.info(f"Initializing private cluster {cluster.scheduler_address} with {nworkers} workers.")
            private_cluster = True
        else:
            logger.info(f"Connecting to cluster {cluster}.")
        client = Client(cluster)
    else:
        client = None

    # Load configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "radiation",
                                  "config_radiation-boxplots.yaml")
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    # Override the first model in the config file if provided in the command line
    models = config['models']
    models[0]['catalog'] = get_arg(args, 'catalog', models[0]['catalog'])
    models[0]['model'] = get_arg(args, 'model', models[0]['model'])
    models[0]['exp'] = get_arg(args, 'exp', models[0]['exp'])
    models[0]['source'] = get_arg(args, 'source', models[0]['source'])
    regrid = get_arg(args, 'regrid', models[0].get('regrid', None))

    logger.debug("Analyzing models:")
    catalog_list, models_list, exp_list, startdate_list, enddate_list, datasets = [], [], [], [], [], []
    variables = config['diagnostic_attributes'].get('variables', ['-tnlwrf', 'tnswrf'])

    for model in models:
        try:
            reader = Reader(catalog=model['catalog'], model=model['model'], exp=model['exp'], source=model['source'],
                            startdate=model['startdate'], enddate=model['enddate'], regrid=regrid, loglevel=loglevel)
            dataset = reader.retrieve()
        except Exception as e:
            logger.error(f"No model data found: {e}")
            logger.critical("Radiation diagnostic is terminated.")
            sys.exit(0)
        datasets.append(dataset)
        catalog_list.append(reader.catalog)
        models_list.append(model['model'])
        exp_list.append(model['exp'])
        startdate_list.append(model.get('startdate') or pd.to_datetime(dataset.time[0].values).strftime('%Y-%m-%d'))
        enddate_list.append(model.get('enddate') or pd.to_datetime(dataset.time[-1].values).strftime('%Y-%m-%d'))

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
        f"Boxplot of radiation variables ({', '.join(variables)}). "
        f"The analysis includes the {models_list[0]} model (experiment {exp_list[0]} from {startdate_list[0]} to {enddate_list[0]}) from {catalog_list[0]}, "
        + ", compared with ".join(
            f"{model} (experiment {exp} from {start} to {end}) from {catalog}"
            for model, exp, start, end, catalog in zip(
                models_list[1:], exp_list[1:], startdate_list[1:], enddate_list[1:], catalog_list[1:]
            )
        ) + "."
        f"This boxplot compares radiation fluxes across different models, highlighting variability and potential biases. "
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

    if client:
        client.close()
        logger.debug("Dask client closed.")

    if private_cluster:
        cluster.close()
        logger.debug("Dask cluster closed.")

    logger.info("Radiation diagnostic completed.")
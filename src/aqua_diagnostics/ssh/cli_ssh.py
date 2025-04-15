# Imports for CLI diagnostic
import sys
import os
import argparse
from dask.distributed import Client, LocalCluster
import pandas as pd
import xarray as xr
import dask.distributed as dd
from dask.utils import format_bytes

from aqua.util import load_yaml, get_arg, OutputSaver, ConfigPath, to_list
from aqua import Reader
from aqua.exceptions import NotEnoughDataError, NoDataError, NoObservationError
from aqua.logger import log_configure
from aqua.diagnostics.ssh import sshVariabilityCompute, sshVariabilityPlot

def parse_arguments(args):
    """Parse command line arguments for Global Biases CLI."""
    parser = argparse.ArgumentParser(description='Global Biases CLI')
    parser.add_argument('-c', '--config', type=str, required=False, help='YAML configuration file')
    parser.add_argument('-n', '--nworkers', type=int, help='Number of Dask distributed workers')
    parser.add_argument("--loglevel", "-l", type=str, help="Logging level")

    # Arguments to override configuration file settings
    parser.add_argument("--catalog", type=str, required=False, help="Catalog name")
    parser.add_argument('--model', type=str, help='Model name')
    parser.add_argument('--exp', type=str, help='Experiment name')
    parser.add_argument('--source', type=str, help='Source name')
    parser.add_argument('--outputdir', type=str, help='Output directory')
    parser.add_argument("--cluster", type=str, required=False, help="dask cluster address")
    parser.add_argument("--regrid", type=str, required=False, help="Regrid the source data to a specified grid")

    return parser.parse_args(args)

def main():
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, 'CLI SSH Variability')
    logger.info("Starting SSH Variablility diagnostic")

    # Load configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "ssh",
                                  "config_ssh.yaml")
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)
    
    # Configuration options for model data
    catalog_data = get_arg(args, 'catalog', config['data']['catalog'])
    model_data = get_arg(args, 'model', config['data']['model'])
    exp_data = get_arg(args, 'exp', config['data']['exp'])
    source_data = get_arg(args, 'source', config['data']['source'])
    startdate_data = config['data'].get('startdate')
    enddate_data = config['data'].get('enddate')
    #zoom = get_arg(args, 'zoom', config['data']['zoom'])

    # Configuation options for observational data 
    catalog_obs = get_arg(args, 'catalog', config['obs']['catalog'])
    model_obs = get_arg(args, 'model', config['obs']['model'])
    exp_obs = get_arg(args, 'exp', config['obs']['exp'])
    source_obs = get_arg(args, 'source', config['obs']['source'])
    startdate_obs = config['obs'].get('startdate')
    enddate_obs = config['obs'].get('enddate')

    outputdir = get_arg(args, 'outputdir', config['outputdir'])
    plot_options = config.get("plot_options", {})
    mask_options = config.get("mask_options", {})
    sub_region = config.get("sub_region", {})

    #rebuild = config['output'].get("rebuild")
    #filename = config['output'].get("filename")
    #save_netcdf = config['output'].get("save_netcdf")
    #dpi = config['output'].get("dpi")

    variable = config['variable']

    regrid = get_arg(args, 'regrid', config['regrid'])

    logger.debug(f"Data read from Catalog: {catalog_data}; Model: {model_data}; Exp: {exp_data}; Source: {source_data}; Regrid: {regrid}")
    logger.debug(f"Observations read from Catalog: {catalog_obs}; Model: {model_obs}; Exp: {exp_obs}; Source: {source_obs}; Regrid: {regrid}")

    # Initialize the Dask cluster
    nworkers = get_arg(args, 'nworkers', None)
    config_cluster = get_arg(args, 'cluster', None)
    # If nworkers is not provided, use the value from the config
    if nworkers is None or config_cluster is None:
        config_cluster = config['cluster'].copy()
    if nworkers is not None:
        config_cluster['nworkers'] = nworkers
    cluster = LocalCluster(n_workers=config_cluster['nworkers'], threads_per_worker=1)
    #cluster = LocalCluster(n_workers=config['cluster']['nworkers'], threads_per_worker=1)
    client = Client(cluster)
    # Get the Dask dashboard URL
    logger.info("Dask Dashboard URL: %s", client.dashboard_link)
    workers = client.scheduler_info()["workers"]
    worker_count = len(workers)
    total_memory = format_bytes(
        sum(w["memory_limit"] for w in workers.values() if w["memory_limit"]))
    # total_memory = format_bytes(
    #     sum(config["dask_cluster"]["memory_limit"] for w in workers.values() if "memory_limit" in config["dask_cluster"]))
    memory_text = f"Workers={worker_count}, Memory={total_memory}"
    logger.info(memory_text)

    # Try loading the precomputed std as xarray.Dataset
    # Else use the sshVariabilityCompute Class

    # Retrieve observational data and handle potential errors
    data_obs_path = f"{outputdir}/ssh/netcdf/{model_obs}_{exp_obs}_{source_obs}_{startdate_obs}_to_{enddate_obs}_std.nc"
    try:
        std_obs = xr.open_dataarray(data_obs_path)
    except:
        logger.info("Preecomputed sshVariability data for %s does not exist. Therefore computing it now", model_obs)
        ssh_std_obs = sshVariabilityCompute(variable=variable, catalog=catalog_obs, model=model_obs, exp=exp_obs, source=source_obs, startdate=startdate_obs, enddate=enddate_obs, regrid=regrid, outputdir=outputdir, loglevel=loglevel) 
        std_obs = ssh_std_obs.run()

    # Retrieve model data and handle potential errors
    data_model_path = f"{outputdir}/ssh/netcdf/{model_data}_{exp_data}_{source_data}_{startdate_data}_to_{enddate_data}_std.nc"
    try:
        std_data = xr.open_dataarray(data_model_path)
    except:
        logger.info("Preecomputed sshVariability data for %s does not exist. Therefore computing it now", model_data)
        ssh_std_data = sshVariabilityCompute(variable=variable, catalog=catalog_data, model=model_data, exp=exp_data, source=source_data, startdate=startdate_data, enddate=enddate_data, regrid=regrid, outputdir=outputdir, loglevel=loglevel) 
        std_data = ssh_std_data.run()

    # SSH variability Plotting
    ssh_plot = sshVariabilityPlot(variable=variable, data_ref=std_obs, data_model=std_data, name_ref=model_obs, name_model=model_data, exp_ref=exp_obs, exp_model=exp_data, source_ref=source_obs, source_model=source_data, startdate_model=startdate_data, enddate_model=enddate_data, startdate_ref=startdate_obs, enddate_ref=enddate_obs, outputdir=outputdir, plot_options=plot_options, sub_region=sub_region, mask_options=mask_options)
    ssh_plot.run()

    logger.info("Finished SSH diagnostic.")
    # Close the Dask client and cluster
    client.close()
    cluster.close()

if __name__ == '__main__':
    main()

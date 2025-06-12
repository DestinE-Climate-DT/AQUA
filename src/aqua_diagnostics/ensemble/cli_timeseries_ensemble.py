#!/usr/bin/env python3
"""
Command-line interface for ensemble global time series diagnostic.

This CLI allows to plot ensemle of global timeseries of a variable
defined in a yaml configuration file for multiple models.
"""
import argparse
import os
import sys

import xarray as xr
from aqua.diagnostics import EnsembleTimeseries
from aqua.logger import log_configure
from aqua.util import ConfigPath, get_arg, load_yaml
from dask.distributed import Client, LocalCluster
from dask.utils import format_bytes
from aqua.diagnostics.ensemble.util import retrieve_merge_ensemble_data

def parse_arguments(args):
    """Parse command line arguments."""

    parser = argparse.ArgumentParser(description="Global time series CLI")

    parser.add_argument("-c", "--config", type=str, required=False, help="yaml configuration file")
    parser.add_argument("-n", "--nworkers", type=int, help="number of dask distributed workers")
    parser.add_argument("--loglevel", "-l", type=str, required=False, help="loglevel")

    # These will override the first one in the config file if provided
    parser.add_argument("--catalog", type=str, required=False, help="catalog name")
    parser.add_argument("--model", type=str, required=False, help="model name")
    parser.add_argument("--exp", type=str, required=False, help="experiment name")
    parser.add_argument("--source", type=str, required=False, help="source name")
    parser.add_argument("--startdate", type=str, required=False, help="start date")
    parser.add_argument("--enddate", type=str, required=False, help="end date")
    parser.add_argument("--outputdir", type=str, required=False, help="output directory")
    parser.add_argument("--cluster", type=str, required=False, help="dask cluster address")

    return parser.parse_args(args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, "CLI multi-model Timeseries ensemble")
    logger.info("Starting Ensemble Time Series diagnostic")

    # Load configuration file
    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(
        configdir, "diagnostics", "ensemble", "config_timeseries_ensemble.yaml"
    )
    file = get_arg(args, "config", default_config)
    logger.info(f"Reading configuration file {file}")
    config = load_yaml(file)

    # Initialize the Dask cluster
    nworkers = get_arg(args, "nworkers", None)
    config_cluster = get_arg(args, "cluster", None)

    # If nworkers is not provided, use the value from the config
    if nworkers is None or config_cluster is None:
        config_cluster = config["cluster"].copy()
    if nworkers is not None:
        config_cluster["nworkers"] = nworkers

    cluster = LocalCluster(n_workers=config_cluster["nworkers"], threads_per_worker=1)
    client = Client(cluster)

    # Get the Dask dashboard URL
    logger.info("Dask Dashboard URL: %s", client.dashboard_link)
    workers = client.scheduler_info()["workers"]
    worker_count = len(workers)
    total_memory = format_bytes(
        sum(w["memory_limit"] for w in workers.values() if w["memory_limit"])
    )
    memory_text = f"Workers={worker_count}, Memory={total_memory}"
    logger.info(memory_text)

    variable = config["variable"]
    logger.info(f"Variable under consideration: {variable}")
    outputdir = get_arg(args, "outputdir", config["outputdir"])

    startdate = get_arg(args, "startdate", config["startdate"])
    enddate = get_arg(args, "enddate", config["enddate"])

    plot_options = config.get("plot_options", {})

    logger.info(f"Loading {variable} timeseries")

    # Model data
    models = config["datasets"]

    mon_model_list = []
    mon_exp_list = []
    mon_source_list = []

    ann_model_list = []
    ann_exp_list = []
    ann_source_list = []

    if models is not None:
        models[0]["model"] = get_arg(args, "model", models[0]["model"])
        models[0]["exp"] = get_arg(args, "exp", models[0]["exp"])
        models[0]["source"] = get_arg(args, "source", models[0]["source"])
        for model in models:
            if model["source"] == "aqua-timeseries-monthly":
                mon_model_list.append(model["model"])
                mon_exp_list.append(model["exp"])
                mon_source_list.append(model["source"])
            if model["source"] == "aqua-timeseries-annual":
                ann_model_list.append(model["model"])
                ann_exp_list.append(model["exp"])
                ann_source_list.append(model["source"])

    # Reterive monthly data
    mon_dataset = retrieve_merge_ensemble_data(
        variable=variable,
        models_catalog_list=mon_model_list,
        exps_catalog_list=mon_exp_list,
        sources_catalog_list=mon_source_list,
    )

    # Reterieve annual data
    ann_dataset = retrieve_merge_ensemble_data(
        variable=variable,
        models_catalog_list=ann_model_list,
        exps_catalog_list=ann_exp_list,
        sources_catalog_list=ann_source_list,
    )

    ## Reference monthly data
    # ref = config["reference"]
    # ref[0]["model"] = get_arg(args, "model", ref[0]["model"])
    # ref[0]["exp"] = get_arg(args, "exp", ref[0]["exp"])
    # ref[0]["source"] = get_arg(args, "source", ref[0]["source"])
    # for ref_model in ref:
    #    if ref is not None and ref_model["source"] == "aqua-timeseries-monthly":
    #        ref_mon_model = ref_model["model"]
    #        ref_mon_exp = ref_model["exp"]
    #        ref_mon_source = ref_model["source"]
    #    if ref is not None and ref_model["source"] == "aqua-timeseries-annual":
    #        ref_ann_model = ref_model["model"]
    #        ref_ann_exp = ref_model["exp"]
    #        ref_ann_source = ref_model["source"]

    # reader = Reader(
    #    model=ref_mon_model,
    #    exp=ref_mon_exp,
    #    source=ref_mon_source,
    #    startdate=mon_startdate,
    #    enddate=mon_enddate,
    #    areas=False,
    #    variable=variable,
    # )
    # ref_mon_dataset = reader.retrieve(var=variable)

    # reader = Reader(
    #    model=ref_ann_model,
    #    exp=ref_ann_exp,
    #    source=ref_ann_source,
    #    startdate=ann_startdate,
    #    enddate=ann_enddate,
    #    areas=False,
    #    variable=variable,
    # )
    # ref_ann_dataset = reader.retrieve(var=variable)

    # loading the reference data as xarrays

    # Monthly reference data
    ERA5_mon = "/work/ab0995/a270260/pre_computed_aqua_analysis/IFS-FESOM/historical-1990/global_time_series/netcdf/global_time_series_timeseries_2t_ERA5_era5_mon.nc"
    mon_ref_data = xr.open_dataset(
        ERA5_mon,
        drop_variables=[var for var in xr.open_dataset(ERA5_mon).data_vars if var != variable],
    )
    # selection ERA5 data on the same time interval -> xarray.DataArray
    mon_ref_data = mon_ref_data[variable].sel(time=slice(mon_dataset.time[0], mon_dataset.time[-1]))

    # Annual reference data
    ERA5_ann = "/work/ab0995/a270260/pre_computed_aqua_analysis/IFS-FESOM/historical-1990/global_time_series/netcdf/global_time_series_timeseries_2t_ERA5_era5_ann.nc"
    ann_ref_data = xr.open_dataset(
        ERA5_ann,
        drop_variables=[var for var in xr.open_dataset(ERA5_ann).data_vars if var != variable],
    )
    # selection ERA5 data on the same time interval -> xarray.DataArray
    ann_ref_data = ann_ref_data[variable].sel(time=slice(ann_dataset.time[0], ann_dataset.time[-1]))

    # Check if we need monthly and annual time variables

    ts = EnsembleTimeseries(
        var=variable,
        mon_model_dataset=mon_dataset,
        ann_model_dataset=ann_dataset,
        mon_ref_data=mon_ref_data,
        ann_ref_data=ann_ref_data,
        plot_options=plot_options,
    )

    ts.compute()
    ts.plot()

    logger.info(f"Finished Ensemble time series diagnostic for {variable}.")
    # Close the Dask client and cluster
    client.close()
    cluster.close()

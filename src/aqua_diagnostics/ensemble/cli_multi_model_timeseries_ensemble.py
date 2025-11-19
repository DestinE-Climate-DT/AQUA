#!/usr/bin/env python3
"""
Command-line interface for ensemble global time series diagnostic.

This CLI allows to plot ensemle of global timeseries of a variable
defined in a yaml configuration file for multiple models.

NOTE: Since the reference data is not in the catalog the data is loaded from the path
      Once the reference data is uploaded in the catalog, line 170-192 can be un-commented
      and line 194-217 can be removed/commented.
"""

import argparse
import sys

import xarray as xr
from aqua.diagnostics import EnsembleTimeseries, PlotEnsembleTimeseries, reader_retrieve_and_merge
from aqua.diagnostics.core import (
    close_cluster,
    load_diagnostic_config,
    merge_config_args,
    open_cluster,
    template_parse_arguments,
)
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version


def parse_arguments(args):
    """Parse command-line arguments for EnsembleTimeseries diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description="EnsembleTimeseries CLI")
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, "CLI multi-model Timeseries ensemble")
    logger.info("Starting Ensemble Time Series diagnostic")

    cluster = get_arg(args, "cluster", None)
    nworkers = get_arg(args, "nworkers", None)

    (
        client,
        cluster,
        private_cluster,
    ) = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments
    config_dict = load_diagnostic_config(
        diagnostic="ensemble",
        config=args.config,
        default_config="config_multi_model_timeseries_ensemble.yaml",
        loglevel=loglevel,
    )
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    # rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict["output"].get("save_netcdf", True)
    save_pdf = config_dict["output"].get("save_pdf", True)
    save_png = config_dict["output"].get("save_png", True)
    # dpi = config_dict['output'].get('dpi', 300)

    # EnsembleTimeseries diagnostic
    if "ensemble" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["ensemble"]["run"]:
            logger.info("EnsembleTimeseries module is used.")

            reference = config_dict["references"][0]
            # Loop over all the variables in the config file
            for variable in config_dict["diagnostics"]["ensemble"].get("variable", None):
                logger.info(f"Variable under consideration: {variable}")

                startdate_data = config_dict["diagnostics"]["ensemble"]["params"]["default"].get("startdate_data", None)
                enddate_data = config_dict["diagnostics"]["ensemble"]["params"]["default"].get("enddate_data", None)
                startdate_ref = config_dict["diagnostics"]["ensemble"]["params"]["default"].get("startdate_ref", None)
                enddate_ref = config_dict["diagnostics"]["ensemble"]["params"]["default"].get("enddate_ref", None)
                title = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("title", None)
                plot_ensemble_members = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get(
                    "plot_ensemble_members", True
                )

                # Model data
                # TODO: hourly and daily data
                models = config_dict["datasets"]

                monthly_catalog_list = []
                monthly_model_list = []
                monthly_exp_list = []
                monthly_source_list = []
                # All the realizations will be appended here with the key of model names
                monthly_realization_dict = {}

                annual_catalog_list = []
                annual_model_list = []
                annual_exp_list = []
                annual_source_list = []
                # All the realizations will be appended here with the key of model names
                annual_realization_dict = {}

                if models is not None:
                    models[0]["catalog"] = get_arg(args, "catalog", models[0]["catalog"])
                    models[0]["model"] = get_arg(args, "model", models[0]["model"])
                    models[0]["exp"] = get_arg(args, "exp", models[0]["exp"])
                    models[0]["source"] = get_arg(args, "source", models[0]["source"])
                    models[0]["regrid"] = get_arg(args, "regrid", models[0]["regrid"])
                    models[0]["realization"] = get_arg(args, "realization", models[0]["realization"])
                    for model in models:
                        if model["source"] == "aqua-timeseries-monthly":
                            monthly_catalog_list.append(model["catalog"])
                            monthly_model_list.append(model["model"])
                            monthly_exp_list.append(model["exp"])
                            monthly_source_list.append(model["source"])
                            monthly_realization_dict.update({model["model"]: model["realization"]})

                        if model["source"] == "aqua-timeseries-annual":
                            annual_catalog_list.append(model["catalog"])
                            annual_model_list.append(model["model"])
                            annual_exp_list.append(model["exp"])
                            annual_source_list.append(model["source"])
                            annual_realization_dict.update({model["model"]: model["realization"]})

                # Reterive monthly data
                monthly_dataset = reader_retrieve_and_merge(
                    variable=variable,
                    catalog_list=monthly_catalog_list,
                    model_list=monthly_model_list,
                    exp_list=monthly_exp_list,
                    source_list=monthly_source_list,
                    regrid=models[0]["regrid"],
                    realization=monthly_realization_dict,
                    startdate=startdate_data,
                    enddate=enddate_data,
                )

                if monthly_dataset is None:
                    logger.warning("Monthly ensemble data is not provided.")

                # Reterieve annual data
                annual_dataset = reader_retrieve_and_merge(
                    variable=variable,
                    catalog_list=annual_catalog_list,
                    model_list=annual_model_list,
                    exp_list=annual_exp_list,
                    source_list=annual_source_list,
                    regrid=models[0]["regrid"],
                    realization=annual_realization_dict,
                    startdate=startdate_data,
                    enddate=enddate_data,
                )
                if annual_dataset is None:
                    logger.warning("Annual ensemble data is not provided.")

                # Reference monthly data
                ref = config_dict["references"]
                ref[0]["catalog"] = get_arg(args, "catalog", ref[0]["catalog"])
                ref[0]["model"] = get_arg(args, "model", ref[0]["model"])
                ref[0]["exp"] = get_arg(args, "exp", ref[0]["exp"])
                ref[0]["source"] = get_arg(args, "source", ref[0]["source"])
                for ref_model in ref:
                    if ref is not None and ref_model["source"] == "aqua-timeseries-monthly":
                        ref_monthly_catalog = ref_model["catalog"]
                        ref_monthly_model = ref_model["model"]
                        ref_monthly_exp = ref_model["exp"]
                        ref_monthly_source = ref_model["source"]
                    if ref is not None and ref_model["source"] == "aqua-timeseries-annual":
                        ref_annual_catalog = ref_model["catalog"]
                        ref_annual_model = ref_model["model"]
                        ref_annual_exp = ref_model["exp"]
                        ref_annual_source = ref_model["source"]

                ## Monthly reference data
                # reader = Reader(
                #    model=ref_monthly_model,
                #    exp=ref_monthly_exp,
                #    source=ref_monthly_source,
                #    startdate=monthly_startdate,
                #    enddate=monthly_enddate,
                #    areas=False,
                #    variable=variable,
                # )
                # monthly_ref_data = reader.retrieve(var=variable)

                ## Annual reference data
                # reader = Reader(
                #    model=ref_annual_model,
                #    exp=ref_annual_exp,
                #    source=ref_annual_source,
                #    startdate=annual_startdate,
                #    enddate=annual_enddate,
                #    areas=False,
                #    variable=variable,
                # )
                # annual_ref_data = reader.retrieve(var=variable)

                # Monthly reference data
                ERA5_monthly = "/work/ab0995/a270260/pre_computed_aqua_analysis/IFS-FESOM/historical-1990/global_time_series/netcdf/global_time_series_timeseries_2t_ERA5_era5_mon.nc"
                monthly_ref_data = xr.open_dataset(
                    ERA5_monthly,
                    drop_variables=[var for var in xr.open_dataset(ERA5_monthly).data_vars if var != variable],
                )
                # selection ERA5 data on the same time interval -> xarray.DataArray
                # monthly_ref_data = monthly_ref_data[variable].sel(time=slice(monthly_dataset.time[0], monthly_dataset.time[-1]))
                monthly_ref_data = monthly_ref_data[variable].sel(time=slice(startdate_ref, enddate_ref))
                # Annual reference data
                ERA5_annual = "/work/ab0995/a270260/pre_computed_aqua_analysis/IFS-FESOM/historical-1990/global_time_series/netcdf/global_time_series_timeseries_2t_ERA5_era5_ann.nc"
                annual_ref_data = xr.open_dataset(
                    ERA5_annual,
                    drop_variables=[var for var in xr.open_dataset(ERA5_annual).data_vars if var != variable],
                )
                # selection ERA5 data on the same time interval -> xarray.DataArray
                # annual_ref_data = annual_ref_data[variable].sel(time=slice(annual_dataset.time[0], annual_dataset.time[-1]))
                annual_ref_data = annual_ref_data[variable].sel(time=slice(startdate_ref, enddate_ref))

                # Check if we need monthly and annual time variables
                ts = EnsembleTimeseries(
                    var=variable,
                    monthly_data=monthly_dataset,
                    annual_data=annual_dataset,
                    catalog_list=monthly_catalog_list,
                    model_list=monthly_model_list,
                    exp_list=monthly_exp_list,
                    source_list=monthly_source_list,
                    outputdir=outputdir,
                    loglevel=loglevel,
                )

                # Compute statistics and save the results as netcdf
                ts.run()

                # Initializing PlotEnsembleTimeseries class
                plot_class_arguments = {
                    "catalog_list": monthly_catalog_list,
                    "model_list": monthly_model_list,
                    "exp_list": monthly_exp_list,
                    "source_list": monthly_source_list,
                    "ref_catalog": ref_monthly_catalog,
                    "ref_model": ref_monthly_model,
                    "ref_exp": ref_monthly_exp,
                }

                ts_plot = PlotEnsembleTimeseries(
                    **plot_class_arguments,
                    outputdir=outputdir,
                    loglevel=loglevel,
                )

                # PlotEnsembleTimeseries plot options
                plot_arguments = {
                    "var": variable,
                    "monthly_data": ts.monthly_data,
                    "monthly_data_mean": ts.monthly_data_mean,
                    "monthly_data_std": ts.monthly_data_std,
                    "annual_data": ts.annual_data,
                    "annual_data_mean": ts.annual_data_mean,
                    "annual_data_std": ts.annual_data_std,
                    "ref_monthly_data": monthly_ref_data,
                    "ref_annual_data": annual_ref_data,
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "plot_ensemble_members": plot_ensemble_members,
                    "title": title,
                    "startdate": ts.monthly_data.time.isel(time=0).values,
                    "enddate": ts.monthly_data.time.isel(time=-1).values,
                }

                # plot() function in PlotEnsembleTimeseries class
                ensemble_plot = ts_plot.plot(**plot_arguments)

                logger.info(f"Finished Ensemble time series diagnostic for {variable}.")

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

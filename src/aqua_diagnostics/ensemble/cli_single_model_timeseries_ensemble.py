#!/usr/bin/env python3
"""
Command-line interface for ensemble global time series diagnostic.

This CLI allows to plot ensemble of global timeseries of a variable
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys

from aqua import Reader
from aqua.diagnostics import EnsembleTimeseries, PlotEnsembleTimeseries, reader_retrieve_and_merge, extract_realizations
from aqua.diagnostics.core import (
    close_cluster, load_diagnostic_config, merge_config_args,
    open_cluster, template_parse_arguments,
)
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.configurer import ConfigPath

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
    logger = log_configure(loglevel, "CLI single-model Timeseries ensemble")
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
        default_config="config_single_model_timeseries_ensemble.yaml",
        loglevel=loglevel,
    )
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict["output"].get("save_netcdf", True)
    save_pdf = config_dict["output"].get("save_pdf", True)
    save_png = config_dict["output"].get("save_png", True)
    dpi = config_dict['output'].get('dpi', 300)

    # EnsembleTimeseries diagnostic
    if "ensemble" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["ensemble"]["run"]:
            logger.info("EnsembleTimeseries module is used.")

            # Loop over all the variables in the config file
            for variable in config_dict["diagnostics"]["ensemble"].get("variable", None):
                for region in config_dict["diagnostics"]["ensemble"].get("region") or []:
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
                    dataset = config_dict["datasets"]

                    if dataset is not None:
                        catalog = get_arg(args, "catalog", dataset[0]["catalog"])
                        model = get_arg(args, "model", dataset[0]["model"])
                        exp = get_arg(args, "exp", dataset[0]["exp"])
                        source = get_arg(args, "source", dataset[0]["source"])
                        regrid = get_arg(args, "regrid", dataset[0]["regrid"])
                        realization = extract_realizations(
                            catalog=catalog,
                            model=model,
                            exp=exp,
                            source=source,
                        )
                        realization_dict = {model: realization}
                    # Reterive dataset
                    dataset = reader_retrieve_and_merge(
                        variable=variable,
                        catalog_list=catalog,
                        model_list=model,
                        exp_list=exp,
                        source_list=source,
                        region=region,
                        startdate=startdate_data,
                        enddate=enddate_data,
                        realization=realization_dict,
                    )
                    if dataset is None:
                        logger.warning("Ensemble data is not provided.")

                    # # Reference data
                    # ref = config_dict["references"]
                    # ref_catalog = get_arg(args, "catalog", ref[0]["catalog"])
                    # ref_model = get_arg(args, "model", ref[0]["model"])
                    # ref_exp = get_arg(args, "exp", ref[0]["exp"])
                    # ref_source = get_arg(args, "source", ref[0]["source"])

                    # if ref_catalog is not None and ref_model is not None and ref_exp is not None and ref_source is not None:
                    #     reader = Reader(
                    #         catalog=ref_catalog,
                    #         model=ref_model,
                    #         exp=ref_exp,
                    #         source=ref_source,
                    #         startdate=startdate_ref,
                    #         enddate=enddate_ref,
                    #         region=region,
                    #         areas=False,
                    #         variable=variable,
                    #     )
                    #     ref_data = reader.retrieve(var=variable)
                    #     if ref_data is None:
                    #         logger.warning("Reference data is not provided.")
                    # else:
                    #     logger.warning("Reference catalog, model, exp and source need to be defined")
                    #     ref_data = None

                    if dataset is not None:
                        ts = EnsembleTimeseries(
                            var=variable,
                            monthly_data=dataset,
                            catalog_list=catalog,
                            model_list=model,
                            exp_list=exp,
                            source_list=source,
                            outputdir=outputdir,
                            loglevel=loglevel,
                        )

                        # Compute statistics and save the results as netcdf
                        ts.run()

                    # Initialize PlotEnsembleTimeseries class
                    # if ref_data is not None:
                    #     plot_class_arguments = {
                    #         "catalog_list": catalog,
                    #         "model_list": model,
                    #         "exp_list": exp,
                    #         "source_list": source,
                    #         "ref_catalog": ref_catalog,
                    #         "ref_model": ref_model,
                    #         "ref_exp": ref_exp,
                    #     }
                    # else:
                    plot_class_arguments = {
                        "catalog_list": catalog,
                        "model_list": model,
                        "exp_list": exp,
                        "source_list": source,
                    }

                    if (
                        ts.monthly_data is not None
                        or ts.monthly_data_mean is not None
                        or ts.monthly_data_std is not None
                        or ts.annual_data is not None
                        or ts.annual_data_mean is not None
                        or ts.annual_data_std is not None
                        # or ref_data is not None
                    ):
                        ts_plot = PlotEnsembleTimeseries(
                            **plot_class_arguments,
                            outputdir=outputdir,
                            loglevel=loglevel,
                        )

                        # PlotEnsembleTimeseries plot options
                        # Uncomment the following option and provide their values
                        # as xarray.DataArray
                        plot_arguments = {
                            "var": variable,
                            "monthly_data": ts.monthly_data,
                            "monthly_data_mean": ts.monthly_data_mean,
                            "monthly_data_std": ts.monthly_data_std,
                            # "annual_data": ts.annual_data,
                            # "annual_data_mean": ts.annual_data_mean,
                            # "annual_data_std": ts.annual_data_std,
                            # "ref_monthly_data": ref_data,
                            # "ref_annual_data": ref_annual_data
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

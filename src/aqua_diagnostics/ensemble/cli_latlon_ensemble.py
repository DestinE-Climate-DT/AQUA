#!/usr/bin/env python3
"""
Command-line interface for ensemble 2D Lat-Lon diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys

from aqua.diagnostics import EnsembleLatLon, PlotEnsembleLatLon, reader_retrieve_and_merge
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
    """Parse command-line arguments for EnsembleLatLon diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description="EnsembleLatLon CLI")
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, "CLI multi-model Lat-Lon Ensemble")
    logger.info("Starting Ensemble Lat-Lon diagnostic")

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
        default_config="config_latlon_ensemble.yaml",
        loglevel=loglevel,
    )
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    # rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict["output"].get("save_netcdf", True)
    save_pdf = config_dict["output"].get("save_pdf", True)
    save_png = config_dict["output"].get("save_png", True)
    dpi = config_dict["output"].get("dpi", 300)

    # EnsembleLatLon diagnostic
    if "ensemble" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["ensemble"]["run"]:
            logger.info("EnsembleLatLon module is used.")

            for variable in config_dict["diagnostics"]["ensemble"].get("variable", None):
                logger.info(f"Variable under consideration: {variable}")

                # Model data
                models = config_dict["datasets"]

                catalog_list = []
                model_list = []
                exp_list = []
                source_list = []
                realization_dict = {}
                # TODO:
                reader_kwargs_dict = {}

                if models is not None:
                    models[0]["catalog"] = get_arg(args, "catalog", models[0]["catalog"])
                    models[0]["model"] = get_arg(args, "model", models[0]["model"])
                    models[0]["exp"] = get_arg(args, "exp", models[0]["exp"])
                    models[0]["source"] = get_arg(args, "source", models[0]["source"])
                    models[0]["regrid"] = get_arg(args, "regrid", models[0]["regrid"])
                    models[0]["realization"] = get_arg(args, "realization", models[0]["realization"])
                    #models[0]["fix"] = get_arg(args, "fix", models[0]["fix"])
                    #models[0]["areas"] = get_arg(args, "areas", models[0]["areas"])
                    #model[0]["reader_kwargs"] = get_arg(args, "reader_kwargs", models[0]["reader_kwargs"])
                    for model in models:
                        catalog_list.append(model["catalog"])
                        model_list.append(model["model"])
                        exp_list.append(model["exp"])
                        source_list.append(model["source"])
                        realization_dict.update({model["model"]: model["realization"]})
                        #reader_kwargs_dict.update({model["model"]: {"fix":model["fix"], "areas":model["areas"]}})
 
                # Loading and merging data
                ens_dataset = reader_retrieve_and_merge(
                    variable=variable,
                    catalog_list=catalog_list,
                    model_list=model_list,
                    exp_list=exp_list,
                    source_list=source_list,
                    regrid=models[0]["regrid"],
                    realization=realization_dict,
                    reader_kwargs=reader_kwargs_dict,
                    loglevel="WARNING",
                    ens_dim="ensemble",
                )

                # Initialize EnsembleLatLon class
                ens_latlon = EnsembleLatLon(
                    var=variable,
                    dataset=ens_dataset,
                    catalog_list=catalog_list,
                    exp_list=exp_list,
                    model_list=model_list,
                    source_list=source_list,
                    ensemble_dimension_name="ensemble",
                    outputdir=outputdir,
                )

                ens_latlon.run()

                # Initialize PlotEnsembleLatLon class
                plot_class_arguments = {
                    "catalog_list": catalog_list,
                    "model_list": model_list,
                    "exp_list": exp_list,
                    "source_list": source_list,
                    "outputdir": outputdir,
                }

                all_plot_params = config_dict["diagnostics"]["ensemble"].get("plot_params", {})
                default_params = all_plot_params.get("default", {})
                var_params = all_plot_params.get(variable, {})
                plot_params = {**default_params, **var_params}
                proj = plot_params.get("projection", "robinson")
                proj_params = plot_params.get("projection_params", {})
                cmap = plot_params.get("cmap", "RdBu_r")
                vmin_mean, vmax_mean = plot_params.get("vmin"), plot_params.get("vmax")
                vmin_std, vmax_std = plot_params.get("vmin_std"), plot_params.get("vmax_std")
                param_dict = config_dict["diagnostics"]["ensemble"].get("params", {}).get(variable, {})
                units = param_dict.get("units", None)
                long_name = param_dict.get("long_name", None)
                short_name = param_dict.get("short_name", None)

                ens_latlon_plot = PlotEnsembleLatLon(
                    **plot_class_arguments,
                )

                # PlotEnsembleLatLon plot options
                plot_arguments = {
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "var": variable,
                    "dpi": dpi,
                    "vmin_mean": vmin_mean,
                    "vmax_mean": vmax_mean,
                    "vmin_std": vmin_std,
                    "vmax_std": vmax_std,
                    "proj": proj,
                    "transform_first": False,
                    "cyclic_lon": False,
                    "contour": True,
                    "coastlines": True,
                    "cbar_label": None,
                    "units": units,
                    "dataset_mean": ens_latlon.dataset_mean,
                    "dataset_std": ens_latlon.dataset_std,
                }

                ens_latlon_plot.plot(**plot_arguments)
                logger.info(f"Finished Ensemble_latLon diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

#!/usr/bin/env python3
"""
Command-line interface for ensemble atmglobalmean diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys

from aqua.util import get_arg
from aqua.logger import log_configure
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args

from aqua.diagnostics import retrieve_merge_ensemble_data
from aqua.diagnostics import EnsembleLatLon
from aqua.diagnostics import PlotEnsembleLatLon


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
        default_config="config_global_2D_ensemble.yaml",
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

            variable = config_dict["diagnostics"]["ensemble"].get("variable", None)
            logger.info(f"Variable under consideration: {variable}")
            title_mean = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get(
                "title_mean", None
            )
            title_std = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get(
                "title_std", None
            )
            cbar_label = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get(
                "cbar_label", None
            )

            # Model data
            models = config_dict["datasets"]

            catalog_list = []
            model_list = []
            exp_list = []
            source_list = []
            if models is not None:
                models[0]["catalog"] = get_arg(args, "catalog", models[0]["catalog"])
                models[0]["model"] = get_arg(args, "model", models[0]["model"])
                models[0]["exp"] = get_arg(args, "exp", models[0]["exp"])
                models[0]["source"] = get_arg(args, "source", models[0]["source"])
                for model in models:
                    catalog_list.append(model["catalog"])
                    model_list.append(model["model"])
                    exp_list.append(model["exp"])
                    source_list.append(model["source"])

            ens_dataset = retrieve_merge_ensemble_data(
                variable=variable,
                catalog_list=catalog_list,
                model_list=model_list,
                exp_list=exp_list,
                source_list=source_list,
                log_level="WARNING",
                ens_dim="ensemble",
            )

            ens_latlon = EnsembleLatLon(
                var=variable,
                dataset=ens_dataset,
                catalog_list=catalog_list,
                model_list=model_list,
                source_list=source_list,
                ensemble_dimension_name="ensemble",
            )

            ens_latlon.run()

            # PlotEnsembleLatLon class
            plot_arguments = {
                "var": variable,
                "catalog_list": catalog_list,
                "model_list": model_list,
                "exp_list": exp_list,
                "source_list": source_list,
                "save_pdf": save_pdf,
                "save_png": save_png,
                "title_mean": title_mean,
                "title_std": title_std,
                "cbar_label": cbar_label,
            }

            ens_latlon_plot = PlotEnsembleLatLon(
                **plot_arguments,
                dataset_mean=ens_latlon.dataset_mean,
                dataset_std=ens_latlon.dataset_std,
            )
            ens_latlon_plot.plot()
            logger.info(f"Finished Ensemble_latLon diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(
        client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel
    )

#!/usr/bin/env python3
"""
Command-line interface for ensemble zonalmean diagnostic.

This CLI allows to plot a map of aqua analysis zonalmean
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
from aqua.diagnostics import EnsembleZonal
from aqua.diagnostics import PlotEnsembleZonal

def parse_arguments(args):
    """Parse command-line arguments for EnsembleZonal diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description="EnsembleZonal CLI")
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, "CLI Single model Zonal ensemble")
    logger.info("Starting Ensemble Zonal diagnostic")

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
        default_config="config_zonalmean_ensemble.yaml",
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
            logger.info("EnsembleZonal module is used.")

            variable = config_dict["diagnostics"]["ensemble"].get("variable", None)
            region = config_dict["diagnostics"]["ensemble"].get("region", None)

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
            figure_size = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get(
                "figure_size", None
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
                region=region,
                variable=variable,
                model_list=model_list,
                exp_list=exp_list,
                source_list=source_list,
            )
            ens_zm = EnsembleZonal(
                var=variable,
                dataset=ens_dataset,
                catalog_list=catalog_list,
                model_list=model_list,
                source_list=source_list,
                outputdir=outputdir,
            )
            ens_zm.run()

            # PlotEnsembleZonal class
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
                "figure_size": figure_size,
            }

            ens_zm_plot = PlotEnsembleZonal(
                **plot_arguments, dataset_mean=ens_zm.dataset_mean, dataset_std=ens_zm.dataset_std
            )
            ens_zm_plot.plot()
            logger.info(f"Finished Ensemble_Zonal diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(
        client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel
    )

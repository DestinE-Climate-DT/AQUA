#!/usr/bin/env python3
"""
Command-line interface for ensemble zonalmean diagnostic.

This CLI allows to plot a map of aqua analysis zonalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys

from aqua.diagnostics import EnsembleZonal, PlotEnsembleZonal, reader_retrieve_and_merge
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
    # dpi = config_dict["output"].get("dpi", 300)

    # EnsembleZonal diagnostic
    if "ensemble" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["ensemble"]["run"]:
            logger.info("EnsembleZonal module is used.")

            for variable in config_dict["diagnostics"]["ensemble"].get("variable", None):
                for region in config_dict["diagnostics"]["ensemble"].get("region", None):

                    logger.info(f"Variable under consideration: {variable}")
                    title_mean = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("title_mean", None)
                    title_std = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("title_std", None)
                    cbar_label = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("cbar_label", None)
                    figure_size = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("figure_size", None)

                    # Model data
                    models = config_dict["datasets"]

                    catalog_list = []
                    model_list = []
                    exp_list = []
                    source_list = []
                    realization_dict = {}
                    if models is not None:
                        models[0]["catalog"] = get_arg(args, "catalog", models[0]["catalog"])
                        models[0]["model"] = get_arg(args, "model", models[0]["model"])
                        models[0]["exp"] = get_arg(args, "exp", models[0]["exp"])
                        models[0]["source"] = get_arg(args, "source", models[0]["source"])
                        models[0]["realization"] = get_arg(args, "realization", models[0]["realization"])
                        for model in models:
                            catalog_list.append(model["catalog"])
                            model_list.append(model["model"])
                            exp_list.append(model["exp"])
                            source_list.append(model["source"])
                            realization_dict.update({model["model"]: model["realization"]})

                    # Loading and merging data
                    ens_dataset = reader_retrieve_and_merge(
                        region=region,
                        variable=variable,
                        catalog_list=catalog_list,
                        model_list=model_list,
                        exp_list=exp_list,
                        source_list=source_list,
                        regrid=False,
                        areas=False,
                        fix=True,
                        realization=realization_dict,
                        ens_dim="ensemble",
                    )

                    # Initialize EnsembleZonal class
                    ens_zm = EnsembleZonal(
                        var=variable,
                        dataset=ens_dataset,
                        catalog_list=catalog_list,
                        model_list=model_list,
                        exp_list=exp_list,
                        source_list=source_list,
                        outputdir=outputdir,
                    )
                    ens_zm.run()

                    # Initialize PlotEnsembleZonal class
                    plot_class_arguments = {
                        "catalog_list": catalog_list,
                        "model_list": model_list,
                        "exp_list": exp_list,
                        "source_list": source_list,
                        "outputdir": outputdir,
                    }

                    ens_zm_plot = PlotEnsembleZonal(**plot_class_arguments)

                    # PlotEnsembleLatLon plot options
                    plot_arguments = {
                        "save_pdf": save_pdf,
                        "save_png": save_png,
                        "var": variable,
                        "cbar_label": None,
                    }

                    ens_zm_plot.plot(**plot_arguments, dataset_mean=ens_zm.dataset_mean, dataset_std=ens_zm.dataset_std)
                    logger.info(f"Finished Ensemble_Zonal diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

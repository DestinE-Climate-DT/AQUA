#!/usr/bin/env python3
"""
Command-line interface for ensemble global bias 2D lat-lon diagnostic.

This CLI allows to plot ensemble of global bias 2D lat-lon of a variable
defined in a yaml configuration file for a single model.
"""
import argparse
import sys

from aqua import Reader
from aqua.diagnostics import EnsembleLatLon, PlotEnsembleLatLon, reader_retrieve_and_merge
from aqua.diagnostics.core import (
    close_cluster, load_diagnostic_config, merge_config_args,
    open_cluster, template_parse_arguments,
)
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.configurer import ConfigPath
# This is no circular import because this is a CLI so far
from aqua.diagnostics.ensemble import extract_realizations

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
    logger = log_configure(loglevel, "CLI single-model Global Bias 2D lat-lon ensemble")
    logger.info("Starting Ensemble Global Bias (lat-lon) diagnostic")

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
        default_config="config_single_model_latlon_ensemble.yaml",
        loglevel=loglevel,
    )

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict["output"].get("save_netcdf", True)
    save_pdf = config_dict["output"].get("save_pdf", True)
    save_png = config_dict["output"].get("save_png", True)
    dpi = config_dict['output'].get('dpi', 300)

    # EnsembleLatLon diagnostic
    if "ensemble" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["ensemble"]["run"]:
            logger.info("EnsembleLatLon module is used.")

            # Loop over all the variables in the config file
            for variable in config_dict["diagnostics"]["ensemble"].get("variable", None):
                logger.info(f"Variable under consideration: {variable}")

                title = config_dict["diagnostics"]["ensemble"]["plot_params"]["default"].get("title", None)

                # Model data
                # TODO: region parameter
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
                    #region=region,
                    realization=realization_dict,
                )
                if dataset is None:
                    logger.warning("Ensemble data is not provided.")

                if dataset is not None:
                    ens_latlon = EnsembleLatLon(
                        var=variable,
                        dataset=dataset,
                        catalog_list=catalog,
                        model_list=model,
                        exp_list=exp,
                        source_list=source,
                        outputdir=outputdir,
                        loglevel=loglevel,
                    )

                    # Compute statistics and save the results as netcdf
                    ens_latlon.run()

                # Initialize PlotEnsembleLatLon class
                plot_class_arguments = {
                    "catalog_list": catalog,
                    "model_list": model,
                    "exp_list": exp,
                    "source_list": source,
                    "outputdir": outputdir,
                    "loglevel": loglevel,
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

                logger.info(f"Finished EnsembleLatLon diagnostic for {variable}.")
    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

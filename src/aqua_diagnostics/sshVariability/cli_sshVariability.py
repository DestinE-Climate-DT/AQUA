#!/usr/bin/env python3
"""
Command-line interface for ensemble atmglobalmean diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys
from aqua.diagnostics import sshVariabilityCompute, sshVariabilityPlot
from aqua.diagnostics.core import (close_cluster, load_diagnostic_config, merge_config_args,
                                   open_cluster, template_parse_arguments)
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version


def parse_arguments(args):
    """Parse command-line arguments for sshVariability diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description="sshVariability CLI")
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, "loglevel", "WARNING")
    logger = log_configure(loglevel, "CLI for sshVariability")
    logger.info("Starting SSH Variability diagnostic")
    cluster = get_arg(args, "cluster", None)
    nworkers = get_arg(args, "nworkers", None)
    (
        client,
        cluster,
        private_cluster,
    ) = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments
    config_dict = load_diagnostic_config(
        diagnostic="sshVariability",
        config=args.config,
        default_config="config_ssh.yaml",
        loglevel=loglevel,
    )
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    realization = get_arg(args, 'realization', None)
    if realization:
        logger.info(f"Realization option is set to {realization}")
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = {}
    zoom = get_arg(args, 'zoom', None)
    if zoom:
        logger.info(f"zoom option is set to {zoom}")
        reader_kwargs = {'zoom': zoom}

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    rebuild = config_dict['output'].get('rebuild', True)
    save_netcdf = config_dict["output"].get("save_netcdf", True)
    save_pdf = config_dict["output"].get("save_pdf", True)
    save_png = config_dict["output"].get("save_png", True)
    dpi = config_dict["output"].get("dpi", 600)

    if "sshVariability" in config_dict["diagnostics"]:
        if config_dict["diagnostics"]["sshVariability"]["run"]:
            logger.info("sshVariability module is used.")

            # Model data
            dataset = config_dict["datasets"][0]
            if dataset is not None:
                dataset_dict = {"catalog" : dataset["catalog"], "model" : dataset["model"], "exp" : dataset["exp"], "source" : dataset["source"], "regrid" : dataset["regrid"]}

            # Reference data
            dataset_ref = config_dict["references"][0]
            if dataset_ref is not None:
                dataset_dict_ref = {"catalog" : dataset_ref["catalog"], "model" : dataset_ref["model"], "exp" : dataset_ref["exp"], "source" : dataset_ref["source"], "regrid" : dataset_ref["regrid"]}

            variable = config_dict["diagnostics"]["sshVariability"].get("variables", None)
            logger.info(f"Variable under consideration: {variable}")
            startdate_data = config_dict['diagnostics']["sshVariability"]['params']['default'].get('startdate_data', None)
            enddate_data = config_dict['diagnostics']["sshVariability"]['params']['default'].get('enddate_data', None)
            startdate_ref = config_dict['diagnostics']["sshVariability"]['params']['default'].get('startdate_ref', None)
            enddate_ref = config_dict['diagnostics']["sshVariability"]['params']['default'].get('enddate_ref', None)
            if dataset["zoom"]:
                logger.info(f"zoom option is set to {zoom}")
                reader_kwargs = {'zoom': dataset["zoom"]}

            # Initialize SSH Variability for model dataset
            ssh_dataset = sshVariabilityCompute(
                **dataset_dict,
                var=variable,
                startdate=startdate_data,
                enddate=enddate_data,
                reader_kwargs=reader_kwargs,
            )
            # Perform computation here for model dataset
            ssh_dataset.run()
            
            # Initialize SSH Variability for reference dataset
            ssh_ref = sshVariabilityCompute(
                **dataset_dict_ref,
                var=variable,
                startdate=startdate_ref,
                enddate=enddate_ref,
            )
            # Perform computation here for reference dataset
            ssh_ref.run()
             
            # Initialize plotting class
            ssh_plot = sshVariabilityPlot()
            
            # Dictionary for dataset plot
            plot_arguments_dataset = {
                "var": variable,
                "catalog": dataset["catalog"],
                "model": dataset["model"],
                "exp": dataset["exp"],
                "save_pdf": save_pdf,
                "save_png": save_png,
                "startdate": startdate_data,
                "enddate": enddate_data,
            }
            ssh_plot.plot(dataset_std=ssh_dataset.data_std, **plot_arguments_dataset)

            # Dictionary for reference plot
            plot_arguments_ref = {
                "var": variable,
                "catalog": dataset_ref["catalog"],
                "model": dataset_ref["model"],
                "exp": dataset_ref["exp"],
                "save_pdf": save_pdf,
                "save_png": save_png,
                "startdate": startdate_ref,
                "enddate": enddate_ref,
            }
            ssh_plot.plot(dataset_std=ssh_ref.data_std, **plot_arguments_ref)
           
            # Dictionary for difference of sshVariability plot
            plot_arguments_diff = {
                "var": variable,
                "catalog": dataset["catalog"],
                "model": dataset["model"],
                "exp": dataset["exp"],
                "catalog_ref": dataset_ref["catalog"],
                "model_ref": dataset_ref["model"],
                "exp_ref": dataset_ref["exp"],
                "save_pdf": save_pdf,
                "save_png": save_png,
                "startdate": startdate_ref,
                "enddate": enddate_ref,
            }
            ssh_plot.plot_diff(dataset_std=ssh_dataset.data_std, dataset_std_ref=ssh_ref.data_std, **plot_arguments_diff)

            logger.info(f"Finished SSH Variability diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(
        client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel
    )

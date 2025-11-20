#!/usr/bin/env python3
"""
Command-line interface for ensemble atmglobalmean diagnostic.

This CLI allows to plot a map of aqua analysis atmglobalmean
defined in a yaml configuration file for multiple models.
"""
import argparse
import sys
from aqua.diagnostics import sshVariabilityCompute, sshVariabilityPlot
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
    # If for development: config_ssh_dev.yaml
    config_dict = load_diagnostic_config(
        diagnostic="sshVariability",
        config=args.config,
        default_config="config_ssh.yaml",
        loglevel=loglevel,
    )
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    realization = get_arg(args, "realization", None)
    if realization:
        logger.info(f"Realization option is set to {realization}")
        reader_kwargs = {"realization": realization}
    else:
        reader_kwargs = {}

    # Output options
    outputdir = config_dict["output"].get("outputdir", "./")
    rebuild = config_dict["output"].get("rebuild", True)
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
                dataset_dict = {
                    "catalog": dataset["catalog"],
                    "model": dataset["model"],
                    "exp": dataset["exp"],
                    "source": dataset["source"],
                    "regrid": dataset["regrid"],
                }
            if dataset["zoom"]:
                reader_kwargs.update({"zoom": dataset["zoom"]})

            # Reference data
            dataset_ref = config_dict["references"][0]
            if dataset_ref is not None:
                dataset_dict_ref = {
                    "catalog": dataset_ref["catalog"],
                    "model": dataset_ref["model"],
                    "exp": dataset_ref["exp"],
                    "source": dataset_ref["source"],
                    "regrid": dataset_ref["regrid"],
                }

            variable = config_dict["diagnostics"]["sshVariability"].get("variables", None)
            logger.info(f"Variable under consideration: {variable}")
            startdate_data = config_dict["diagnostics"]["sshVariability"]["params"]["default"].get("startdate_data", None)
            enddate_data = config_dict["diagnostics"]["sshVariability"]["params"]["default"].get("enddate_data", None)
            startdate_ref = config_dict["diagnostics"]["sshVariability"]["params"]["default"].get("startdate_ref", None)
            enddate_ref = config_dict["diagnostics"]["sshVariability"]["params"]["default"].get("enddate_ref", None)

            proj = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("projection", "robinson")
            proj_params = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("projection_params", {})
            logger.debug(f"Using projection: {proj} for variable: {variable}")
            vmin = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("vmin", None)
            vmax = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("vmax", None)
            # Regridder options for plots
            tgt_grid_name = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("tgt_grid_name", None)
            regrid_method = config_dict["diagnostics"]["sshVariability"]["plot_params"]["default"].get("regrid_method", None)

            # Sub region selection
            region_name = config_dict["diagnostics"]["sshVariability"]["plot_params"]["sub_region"].get("name", None)
            region_proj = config_dict["diagnostics"]["sshVariability"]["plot_params"]["sub_region"].get(
                "projection", "plate_carree"
            )
            region_proj_params = config_dict["diagnostics"]["sshVariability"]["plot_params"]["sub_region"].get(
                "projection_params", {}
            )

            lon_limits = config_dict["diagnostics"]["sshVariability"]["plot_params"]["sub_region"].get("lon_limits", None)
            lat_limits = config_dict["diagnostics"]["sshVariability"]["plot_params"]["sub_region"].get("lat_limits", None)

            mask_northern_boundary = config_dict["diagnostics"]["sshVariability"]["plot_params"]["mask_options"].get(
                "mask_northern_boundary", None
            )
            mask_southern_boundary = config_dict["diagnostics"]["sshVariability"]["plot_params"]["mask_options"].get(
                "mask_southern_boundary", None
            )
            northern_boundary_latitude = config_dict["diagnostics"]["sshVariability"]["plot_params"]["mask_options"].get(
                "northern_boundary_latitude", None
            )
            southern_boundary_latitude = config_dict["diagnostics"]["sshVariability"]["plot_params"]["mask_options"].get(
                "southern_boundary_latitude", None
            )

            if dataset["zoom"]:
                logger.info(f"zoom option is set to {dataset["zoom"]}")
                reader_kwargs.update({"zoom": dataset["zoom"]})

            # Initialize SSH Variability for model dataset
            if (
                (dataset_dict["catalog"] is not None)
                or (dataset_dict["model"] is not None)
                or (dataset_dict["exp"] is not None)
                or (dataset_dict["source"] is not None)
            ):
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
            if (
                (dataset_dict_ref["catalog"] is not None)
                or (dataset_dict_ref["model"] is not None)
                or (dataset_dict_ref["exp"] is not None)
                or (dataset_dict_ref["source"] is not None)
            ):
                ssh_ref = sshVariabilityCompute(
                    **dataset_dict_ref,
                    var=variable,
                    startdate=startdate_ref,
                    enddate=enddate_ref,
                    # reader_kwargs=reader_kwargs,
                )
                # Perform computation here for reference dataset
                ssh_ref.run()

            # Initialize plotting class
            plot_class = sshVariabilityPlot()

            # Dictionary for dataset plot
            if ssh_dataset.data_std is not None:
                plot_arguments_dataset = {
                    "var": variable,
                    "catalog": dataset["catalog"],
                    "model": dataset["model"],
                    "exp": dataset["exp"],
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "startdate": startdate_data,
                    "enddate": enddate_data,
                    "proj": proj,
                    "proj_params": proj_params,
                    "vmin": vmin,
                    "vmax": vmax,
                    "tgt_grid_name": tgt_grid_name,
                    "regrid_method": regrid_method,
                }
                plot_class.plot(dataset_std=ssh_dataset.data_std, **plot_arguments_dataset)

            # Dictionary for sub-region dataset plot
            if ssh_dataset.data_std is not None and region_name is not None:
                plot_arguments_dataset = {
                    "var": variable,
                    "catalog": dataset["catalog"],
                    "model": dataset["model"],
                    "exp": dataset["exp"],
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "startdate": startdate_data,
                    "enddate": enddate_data,
                    "proj": region_proj,
                    "proj_params": region_proj_params,
                    "vmin": vmin,
                    "vmax": vmax,
                    "region": region_name,
                    "lon_limits": lon_limits,
                    "lat_limits": lat_limits,
                    "mask_northern_boundary": mask_northern_boundary,
                    "mask_southern_boundary": mask_southern_boundary,
                    "northern_boundary_latitude": northern_boundary_latitude,
                    "southern_boundary_latitude": southern_boundary_latitude,
                    "tgt_grid_name": tgt_grid_name,
                    "regrid_method": regrid_method,
                }
                plot_class.plot(dataset_std=ssh_dataset.data_std, **plot_arguments_dataset)

            # Dictionary for reference plot
            if ssh_ref.data_std is not None:
                plot_arguments_ref = {
                    "var": variable,
                    "catalog": dataset_ref["catalog"],
                    "model": dataset_ref["model"],
                    "exp": dataset_ref["exp"],
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "startdate": startdate_ref,
                    "enddate": enddate_ref,
                    "proj": proj,
                    "proj_params": proj_params,
                    "vmin": vmin,
                    "vmax": vmax,
                    "tgt_grid_name": tgt_grid_name,
                    "regrid_method": regrid_method,
                }
                plot_class.plot(dataset_std=ssh_ref.data_std, **plot_arguments_ref)

            # Dictionary for sub-region reference plot
            if ssh_ref.data_std is not None:
                plot_arguments_ref = {
                    "var": variable,
                    "catalog": dataset_ref["catalog"],
                    "model": dataset_ref["model"],
                    "exp": dataset_ref["exp"],
                    "save_pdf": save_pdf,
                    "save_png": save_png,
                    "startdate": startdate_ref,
                    "enddate": enddate_ref,
                    "region": region_name,
                    "lon_limits": lon_limits,
                    "lat_limits": lat_limits,
                    "proj": region_proj,
                    "proj_params": region_proj_params,
                    "vmin": vmin,
                    "vmax": vmax,
                    "tgt_grid_name": tgt_grid_name,
                    "regrid_method": regrid_method,
                }
                plot_class.plot(dataset_std=ssh_ref.data_std, **plot_arguments_ref)

            # Dictionary for difference of sshVariability plot
            if ssh_dataset.data_std is not None and ssh_ref.data_std is not None:
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
                    "startdate": startdate_data,
                    "enddate": enddate_data,
                    "startdate_ref": startdate_ref,
                    "enddate_ref": enddate_ref,
                    "tgt_grid_name": tgt_grid_name,
                    "regrid_method": regrid_method,
                }
                plot_class.plot_diff(dataset_std=ssh_dataset.data_std, dataset_std_ref=ssh_ref.data_std, **plot_arguments_diff)

            logger.info(f"Finished SSH Variability diagnostic for {variable}.")

    # Close the Dask client and cluster
    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

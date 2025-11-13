#!/usr/bin/env python3
"""
Command-line interface for Ocean stratification diagnostic.

This CLI allows to run the stratification, OceanStratification diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.util import to_list
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics.ocean_stratification.stratification import Stratification
from aqua.diagnostics.ocean_stratification import PlotStratification
from aqua.diagnostics.ocean_stratification import PlotMLD
from aqua.diagnostics.core import DiagnosticCLI


def parse_arguments(args):
    """Parse command-line arguments for OceanStratification diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description="OceanStratification CLI")
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == "__main__":
    args = parse_arguments(sys.argv[1:])
    
    cli = DiagnosticCLI(args, 
                        diagnostic_name='ocean3d', 
                        default_config='config_ocean_stratification.yaml', 
                        log_name='OceanStratification CLI').prepare()
    cli.open_dask_cluster()
    
    logger = cli.logger
    config_dict = cli.config_dict

    dataset = cli.config_dict['datasets'][0]
    dataset_args = cli.dataset_args(dataset)

    if config_dict["references"]:
        references = config_dict["references"]
        logger.info(f"References found: {references}")
        reference = config_dict["references"][0]
        reference_args = cli.dataset_args(reference)

    if "stratification" in config_dict["diagnostics"]["ocean_stratification"]:
        stratification_config = config_dict["diagnostics"]["ocean_stratification"][
            "stratification"
        ]
        logger.info(
            f"Stratification diagnostic is set to {stratification_config['run']}"
        )
        if stratification_config["run"]:
            regions = to_list(stratification_config.get("regions", None))
            diagnostic_name = stratification_config.get(
                "diagnostic_name", "ocean_stratification"
            )
            climatologies = stratification_config.get("climatology", None)
            for region, climatology in zip(regions, climatologies):
                    logger.info(f"Processing region: {region}, climatology: {climatology}")
                    var = stratification_config.get("var", None)
                    # dim_mean = stratification_config.get("dim_mean", ["lat", "lon"])
                    dim_mean = ["lat", "lon"]
                    # Stratification instance
                    # Model data
                    model_stratification = Stratification(
                        **dataset_args,
                        diagnostic_name=diagnostic_name,
                        loglevel=cli.loglevel,
                    )
                    model_stratification.run(
                        region=region,
                        var=var,
                        dim_mean=dim_mean,
                        mld=True,
                        climatology=climatology,
                        outputdir=cli.outputdir,
                        reader_kwargs=cli.reader_kwargs,
                        rebuild=cli.rebuild,
                    )
                    # Reference data
                    if references:
                        logger.info(
                            f"Processing reference data"
                        )
                        obs_stratification = Stratification(
                            **reference_args,
                            diagnostic_name=diagnostic_name,
                            loglevel=cli.loglevel,
                        )
                        obs_stratification.run(
                            region=region,
                            var=var,
                            dim_mean=dim_mean,
                            mld=False,
                            climatology=climatology,
                            outputdir=cli.outputdir,
                            rebuild=cli.rebuild,
                        )
                    else:
                        obs_stratification = None
                    # Plotting Stratification
                    strat_plot = PlotStratification(
                        data=model_stratification.data[["thetao", "so", "rho"]],
                        obs=(
                            obs_stratification.data[["thetao", "so", "rho"]]
                            if obs_stratification is not None
                            else None
                        ),
                        diagnostic_name=diagnostic_name,
                        outputdir=cli.outputdir,
                        loglevel=cli.loglevel,
                    )
                    strat_plot.plot_stratification(
                        save_pdf=cli.save_pdf, save_png=cli.save_png, dpi=cli.dpi
                    )
                    # Mixed Layer Depth instance
                    # Model data
                    model_stratification = Stratification(
                        **dataset_args,
                        diagnostic_name=diagnostic_name,
                        loglevel=cli.loglevel,
                    )
                    model_stratification.run(
                        region=region,
                        var=var,
                        # dim_mean=dim_mean,
                        mld=True,
                        climatology=climatology,
                        outputdir=cli.outputdir,
                        reader_kwargs=cli.reader_kwargs,
                        rebuild=cli.rebuild,
                    )
                    # Reference data
                    if references:
                        logger.info(
                            f"Processing reference data"
                        )
                        obs_stratification = Stratification(
                            **reference_args,
                            diagnostic_name=diagnostic_name,
                            loglevel=cli.loglevel,
                        )
                        obs_stratification.run(
                            region=region,
                            var=var,
                            # dim_mean=dim_mean,
                            mld=True,
                            climatology=climatology,
                            outputdir=cli.outputdir,
                            rebuild=cli.rebuild,
                        )
                    else:
                        obs_stratification = None
                    # Plotting MLD
                    mld_plot = PlotMLD(
                        data=model_stratification.data[["mld"]],
                        obs=(
                            obs_stratification.data[["mld"]]
                            if obs_stratification is not None
                            else None
                        ),
                        diagnostic_name=diagnostic_name,
                        outputdir=cli.outputdir,
                        loglevel=cli.loglevel,
                    )
                    mld_plot.plot_mld(save_pdf=cli.save_pdf, save_png=cli.save_png, dpi=cli.dpi)

    cli.close_dask_cluster()

    logger.info("Ocean stratification diagnostic completed.")

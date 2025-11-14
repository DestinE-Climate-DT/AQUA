#!/usr/bin/env python3
"""
Command-line interface for Ocean drift diagnostic.

This CLI allows to run the hovmoller, OceanDrift diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.util import to_list
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics.ocean_drift.hovmoller import Hovmoller
from aqua.diagnostics.ocean_drift.plot_hovmoller import PlotHovmoller
from aqua.diagnostics.core import DiagnosticCLI


def parse_arguments(args):
    """Parse command-line arguments for OceanDrift diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='OceanDrift CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    
    cli = DiagnosticCLI(args, 
                        diagnostic_name='ocean3d', 
                        default_config='config_ocean_drift.yaml', 
                        log_name='OceanDrift CLI').prepare()
    cli.open_dask_cluster()
    
    logger = cli.logger
    config_dict = cli.config_dict

    dataset = cli.config_dict['datasets'][0]
    dataset_args = cli.dataset_args(dataset)
    
    #logger.info(f"Catalog: {catalog}, Model: {model}, Experiment: {exp}, Source: {source}, Regrid: {regrid}")


    if 'hovmoller' in config_dict['diagnostics']['ocean_drift']:
        hovmoller_config = config_dict['diagnostics']['ocean_drift']['hovmoller']
        logger.info("Hovmoller diagnostic is set to %s", hovmoller_config['run'])
        if hovmoller_config['run']:
            regions = to_list(hovmoller_config.get('regions', None))
            diagnostic_name = hovmoller_config.get('diagnostic_name', 'ocean_drift')
            var = hovmoller_config.get('var', None)
            dim_mean = hovmoller_config.get('dim_mean', ['lat', 'lon'])
            # Add the global region if not present
            # if regions != [None]:
            #    regions.append(None)
            for region in regions:
                logger.info("Processing region: %s", region)
                try:
                    data_hovmoller = Hovmoller(
                        **dataset_args,
                        diagnostic_name=diagnostic_name,
                        loglevel=cli.loglevel
                    )
                    data_hovmoller.run(
                        region=region,
                        var=var,
                        dim_mean=dim_mean,
                        anomaly_ref="t0",
                        outputdir=cli.outputdir,
                        reader_kwargs=cli.reader_kwargs,
                        rebuild=cli.rebuild
                    )
                except Exception as e:
                    logger.error("Error processing region %s: %s", region, e)
                try:
                    logger.info("Loading data in memory")
                    for processed_data in data_hovmoller.processed_data_list:
                        processed_data.load()
                    logger.info("Loaded data in memory")
                    hov_plot = PlotHovmoller(
                        diagnostic_name=diagnostic_name,
                        data=data_hovmoller.processed_data_list,
                        outputdir=cli.outputdir,
                        loglevel=cli.loglevel
                    )
                    hov_plot.plot_hovmoller(
                        rebuild=cli.rebuild, save_pdf=cli.save_pdf,
                        save_png=cli.save_png, dpi=cli.dpi
                    )
                    hov_plot.plot_timeseries(
                        rebuild=cli.rebuild, save_pdf=cli.save_pdf,
                        save_png=cli.save_png, dpi=cli.dpi
                    )
                except Exception as e:
                    logger.error("Error plotting region %s: %s", region, e)
                
    cli.close_dask_cluster()

    logger.info("Ocean Drift diagnostic completed.")

#!/usr/bin/env python3
"""
Command-line interface for Ocean drift diagnostic.

This CLI allows to run the hovmoller, OceanDrift diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.util import get_arg, to_list
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics.ocean_drift.hovmoller import Hovmoller
from aqua.diagnostics.ocean_drift.plot_hovmoller import PlotHovmoller
from aqua_diagnostics.core import DiagnosticCLI


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
    
    cli = DiagnosticCLI(args, 'ocean3d', 'config_ocean_drift.yaml', log_name='OceanDrift CLI').prepare()
    cli.open_dask_cluster()
    
    logger = cli.logger
    config_dict = cli.config_dict
    
    catalog = get_arg(args, 'catalog', config_dict['datasets'][0]['catalog'])
    model = get_arg(args, 'model', config_dict['datasets'][0]['model'])
    exp = get_arg(args, 'exp', config_dict['datasets'][0]['exp'])
    source = get_arg(args, 'source', config_dict['datasets'][0]['source'])
    regrid = get_arg(args, 'regrid', config_dict['datasets'][0]['regrid'])
    startdate = config_dict['datasets'][0].get('startdate', None)
    enddate = config_dict['datasets'][0].get('enddate', None)
    realization = get_arg(args, 'realization', None)
    if realization:
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = config_dict['datasets'][0].get('reader_kwargs') or {}
    logger.info(f"Catalog: {catalog}, Model: {model}, Experiment: {exp}, Source: {source}, Regrid: {regrid}")

    # Output options (from cli_base)
    outputdir = cli.outputdir
    rebuild = cli.rebuild
    save_pdf = cli.save_pdf
    save_png = cli.save_png
    dpi = cli.dpi

    if 'hovmoller' in config_dict['diagnostics']['ocean_drift']:
        hovmoller_config = config_dict['diagnostics']['ocean_drift']['hovmoller']
        logger.info(f"Hovmoller diagnostic is set to {hovmoller_config['run']}")
        if hovmoller_config['run']:
            regions = to_list(hovmoller_config.get('regions', None))
            diagnostic_name = hovmoller_config.get('diagnostic_name', 'ocean_drift')
            var = hovmoller_config.get('var', None)
            dim_mean = hovmoller_config.get('dim_mean', ['lat', 'lon'])
            # Add the global region if not present
            if regions != [None]:
                regions.append(None)
            for region in regions:
                logger.info(f"Processing region: {region}")
                try:
                    data_hovmoller = Hovmoller(
                        diagnostic_name=diagnostic_name,
                        catalog=catalog,
                        model=model,
                        exp=exp,
                        source=source,
                        regrid=regrid,
                        startdate=startdate,
                        enddate=enddate,
                        loglevel=cli.loglevel
                    )
                    data_hovmoller.run(
                        region=region,
                        var=var,
                        dim_mean=dim_mean,
                        anomaly_ref="t0",
                        outputdir=outputdir,
                        reader_kwargs=reader_kwargs,
                        rebuild=rebuild
                    )
                except Exception as e:
                    logger.error(f"Error processing region {region}: {e}")
                try:
                    hov_plot = PlotHovmoller(
                        diagnostic_name=diagnostic_name,
                        data=data_hovmoller.processed_data_list,
                        outputdir=outputdir,
                        loglevel=cli.loglevel
                    )
                    hov_plot.plot_hovmoller(
                        rebuild=rebuild, save_pdf=save_pdf,
                        save_png=save_png, dpi=dpi
                    )
                    hov_plot.plot_timeseries(
                        rebuild=rebuild, save_pdf=save_pdf,
                        save_png=save_png, dpi=dpi
                    )
                except Exception as e:
                    logger.error(f"Error plotting region {region}: {e}")
                
    cli.close_dask_cluster()

    logger.info("OceanDrift diagnostic completed.")

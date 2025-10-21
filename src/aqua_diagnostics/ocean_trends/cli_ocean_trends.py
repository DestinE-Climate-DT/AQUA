#!/usr/bin/env python3
"""
Command-line interface for Ocean trends diagnostic.

This CLI allows to run the trends, OceanTrends diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.util import get_arg
from aqua.diagnostics.core import template_parse_arguments
from aqua.diagnostics.ocean_trends import Trends
from aqua.diagnostics.ocean_trends import PlotTrends
from aqua_diagnostics.core import DiagnosticCLI


def parse_arguments(args):
    """Parse command-line arguments for OceanTrends diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='OceanTrends CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    
    cli = DiagnosticCLI(args, 'ocean3d', 'config_ocean_trends.yaml', log_name='OceanTrends CLI').prepare()
    cli.open_dask_cluster()
    
    logger = cli.logger
    config_dict = cli.config_dict
    
    catalog = get_arg(args, 'catalog', config_dict['datasets'][0]['catalog'])
    model = get_arg(args, 'model', config_dict['datasets'][0]['model'])
    exp = get_arg(args, 'exp', config_dict['datasets'][0]['exp'])
    source = get_arg(args, 'source', config_dict['datasets'][0]['source'])
    regrid = get_arg(args, 'regrid', config_dict['datasets'][0]['regrid'])
    realization = get_arg(args, 'realization', None)
    if realization:
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = config_dict['datasets'][0].get('reader_kwargs', {})
    logger.info(f"Catalog: {catalog}, Model: {model}, Experiment: {exp}, Source: {source}, Regrid: {regrid}")

    # Output options (from cli_base)
    outputdir = cli.outputdir
    rebuild = cli.rebuild
    save_pdf = cli.save_pdf
    save_png = cli.save_png
    dpi = cli.dpi

    formats = []
    if save_pdf:
        formats.append('pdf')
    if save_png:
        formats.append('png')

    if 'multilevel' in config_dict['diagnostics']['ocean_trends']:
        trends_config = config_dict['diagnostics']['ocean_trends']['multilevel']
        logger.info(f"Ocean Trends diagnostic is set to {trends_config['run']}")
        if trends_config['run']:
            regions = trends_config.get('regions', [None])
            diagnostic_name = trends_config.get('diagnostic_name', 'ocean_trends')
            var = trends_config.get('var', None)
            dim_mean = trends_config.get('dim_mean', None) 
            # Add the global region if not present
            if regions != [None] or 'go' not in regions:
                regions.append('go')
            for region in regions:
                logger.info(f"Processing region: {region}")

                try:
                    data_trends = Trends(
                        diagnostic_name=diagnostic_name,
                        catalog=catalog,
                        model=model,
                        exp=exp,
                        source=source,
                        regrid=regrid,
                        loglevel=cli.loglevel
                    )
                    data_trends.run(
                        region=region,
                        var=var,
                        # dim_mean=dim_mean,
                        outputdir=outputdir,
                        rebuild=rebuild,
                    )
                    trends_plot = PlotTrends(
                        data=data_trends.trend_coef,
                        diagnostic_name=diagnostic_name,
                        outputdir=outputdir,
                        rebuild=rebuild,
                        loglevel=cli.loglevel
                    )
                    trends_plot.plot_multilevel(formats=formats, dpi=dpi)
                    
                    zonal_trend_plot = PlotTrends(
                        data=data_trends.trend_coef.mean('lon'),
                        diagnostic_name=diagnostic_name,
                        outputdir=outputdir,
                        rebuild=rebuild,
                        loglevel=cli.loglevel
                    )
                    zonal_trend_plot.plot_zonal(formats=formats, dpi=dpi)
                except Exception as e:
                    logger.error(f"Error processing region {region}: {e}")

    cli.close_dask_cluster()

    logger.info("OceanTrends diagnostic completed.")
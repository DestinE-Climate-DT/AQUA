#!/usr/bin/env python3
"""
Command-line interface for Ocean drift diagnostic.

This CLI allows to run the hovmoller, OceanDrift diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.logger import log_configure
from aqua.util import get_arg, to_list
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args

from aqua.diagnostics.ocean_drift.hovmoller import Hovmoller
from aqua.diagnostics.ocean_drift.plot_hovmoller import PlotHovmoller


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

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='OceanDrift CLI')
    logger.info(f"Running OceanDrift diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge itTimeseries with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='ocean3d',
                                         default_config='config_ocean_drift.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

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

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    if 'hovmoller' in config_dict['diagnostics']['ocean_drift']:
        hovmoller_config = config_dict['diagnostics']['ocean_drift']['hovmoller']
        logger.info(f"Hovmoller diagnostic is set to {hovmoller_config['run']}")
        if hovmoller_config['run']:
            regions = to_list(hovmoller_config.get('regions', None))
            diagnostic_name = hovmoller_config.get('diagnostic_name', 'ocean_drift')
            var = hovmoller_config.get('var', None)
            dim_mean = hovmoller_config.get('dim_mean', ['lat', 'lon'])
            # Add the global region if not present
            # if regions != [None]:
            #     regions.append(None)
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
                        loglevel=loglevel
                    )
                    data_hovmoller.run(
                        region=region,
                        var=var,
                        dim_mean=dim_mean,
                        anomaly_ref= "t0",
                        outputdir=outputdir,
                        # reader_kwargs=reader_kwargs,
                        rebuild=rebuild
                    )
                    hov_plot = PlotHovmoller(
                        diagnostic_name=diagnostic_name,
                        data=data_hovmoller.processed_data_list,
                        outputdir=outputdir,
                        loglevel=loglevel
                    )
                    hov_plot.plot_timeseries(
                        rebuild=rebuild, save_pdf=save_pdf,
                        save_png=save_png, dpi=dpi
                    )
                except Exception as e:
                    logger.error(f"Error processing region {region}: {e}")

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("OceanDrift diagnostic completed.")
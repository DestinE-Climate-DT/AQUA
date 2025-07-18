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
from aqua.util import get_arg
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
    config_dict = load_diagnostic_config(diagnostic='ocean3d', args=args,
                                         default_config='config_ocean_drift.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    catalog = get_arg(args, 'catalog', config_dict['datasets'][0]['catalog'])
    model = get_arg(args, 'model', config_dict['datasets'][0]['model'])
    exp = get_arg(args, 'exp', config_dict['datasets'][0]['exp'])
    source = get_arg(args, 'source', config_dict['datasets'][0]['source'])
    logger.info(f"Catalog: {catalog}, Model: {model}, Experiment: {exp}")
    

    regrid = get_arg(args, 'regrid', config_dict['datasets'][0]['regrid'])
    logger.info(f"Regrid option is set to {regrid}")

    

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
            region = hovmoller_config.get('region', None)
            var = hovmoller_config.get('var', None)
            dim_mean = hovmoller_config.get('dim_mean', None) 
            data_hovmoller = Hovmoller(
                catalog= catalog,
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
                rebuild=rebuild,
            )
            hov_plot = PlotHovmoller(
                data = data_hovmoller.processed_data_list,
                loglevel=loglevel
            )
            hov_plot.plot_hovmoller()

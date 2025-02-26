#!/usr/bin/env python3
"""
Command-line interface for Timeseries diagnostic.

This CLI allows to run the Timeseries, SeasonalCycles and GregoryPlot
diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""
import argparse
import sys

from aqua.logger import log_configure
from aqua.util import get_arg, load_yaml
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.timeseries.util_cli import TimeseriesCLI, SeasonalCyclesCLI, load_var_config
# TODO: update import when #1750 is merged
from aqua.diagnostics.core.util import load_diagnostic_config, merge_config_args

def parse_arguments(args):
    """
    Parse command-line arguments for Timeseries diagnostic.
    
    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='Timeseries CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Timeseries CLI')
    logger.info(f"Running Timeseries diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='timeseries', args=args,
                                         default_config='config_timeseries_atm.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    if 'timeseries' in config_dict['diagnostics']:
        if config_dict['diagnostics']['timeseries']['run']:
            logger.info("Timeseries diagnostic is enabled.")

            for var in config_dict['diagnostics']['timeseries']['variables']:
                var_config, regions = load_var_config(config_dict, var)
                logger.debug(f"Running Timeseries diagnostic for variable {var} with config {var_config}")
                
                for region in regions:
                    logger.debug(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                    ts = TimeseriesCLI(config_dict=config_dict, var=var,
                                    formula=False, loglevel=loglevel)
                    ts.run(regrid=regrid, region=region, outputdir=outputdir,
                           rebuild=rebuild, **var_config)

            for var in config_dict['diagnostics']['timeseries']['formulae']:
                var_config, regions = load_var_config(config_dict, var)
                logger.debug(f"Running Timeseries diagnostic for variable {var} with config {var_config}")

                for region in regions:
                    logger.debug(f"Running Timeseries diagnostic in region {region if region else 'global'}")

                    ts = TimeseriesCLI(config_dict=config_dict, var=var,
                                    formula=True, loglevel=loglevel)
                    ts.run(regrid=regrid, region=region, outputdir=outputdir,
                           rebuild=rebuild, **var_config)
                    
    if 'seasonalcycles' in config_dict['diagnostics']:
        if config_dict['diagnostics']['seasonalcycles']['run']:
            logger.info("SeasonalCycles diagnostic is enabled.")
            
            for var in config_dict['diagnostics']['seasonalcycles']['variables']:
                var_config, regions = load_var_config(config_dict, var)
                logger.debug(f"Running SeasonalCycles diagnostic for variable {var} with config {var_config}")
                
                for region in regions:
                    logger.debug(f"Running SeasonalCycles diagnostic in region {region if region else 'global'}")

                    sc = SeasonalCyclesCLI(config_dict=config_dict, var=var,
                                           formula=False, loglevel=loglevel)
                    sc.run(regrid=regrid, region=region, outputdir=outputdir,
                           rebuild=rebuild, **var_config)

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info("Timeseries diagnostic completed.")
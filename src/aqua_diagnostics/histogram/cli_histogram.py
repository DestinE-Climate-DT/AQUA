#!/usr/bin/env python3
"""
Command-line interface for Histogram diagnostic.

This CLI allows to run the Histogram diagnostic to compute histograms and PDFs
of variables over specified regions. Details of the run are defined in a yaml 
configuration file for single or multiple experiments.
"""

import sys
import argparse
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.histogram.util_cli import load_var_config, process_variable_or_formula


def parse_arguments(args):
    """Parse command-line arguments for Histogram diagnostic.

    Args:
        args (list): list of command-line arguments to parse.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='Histogram CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Histogram CLI')
    logger.info(f"Running Histogram diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)
    
    # Load the configuration file
    try:
        config_dict = load_diagnostic_config(diagnostic='histogram', config=args.config,
                                             default_config='config-histogram.yaml',
                                             loglevel=loglevel)
        if config_dict is None:
            logger.error("Configuration file could not be loaded. Check the config path.")
            sys.exit(1)
    except FileNotFoundError as e:
        logger.error(f"Configuration file not found: {e}")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error loading configuration: {e}")
        sys.exit(1)

    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)
    logger.info(f"Regrid option is set to {regrid}")
    realization = get_arg(args, 'realization', None)
    # This reader_kwargs will be used if the dataset corresponding value is None or not present
    reader_kwargs = config_dict['datasets'][0].get('reader_kwargs') or {}
    if realization:
        reader_kwargs['realization'] = realization

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    # Histogram diagnostic
    if 'histogram' in config_dict['diagnostics']:
        if config_dict['diagnostics']['histogram']['run']:
            logger.info("Histogram diagnostic is enabled.")

            # Extract configuration
            diagnostic_config = config_dict['diagnostics']['histogram']
            
            # Common parameters for all variables/formulae
            common_params = {
                'config_dict': config_dict,
                'datasets': config_dict['datasets'],
                'diagnostic_name': diagnostic_config.get('diagnostic_name', 'histogram'),
                'regrid': regrid,
                'bins': diagnostic_config.get('bins', 100),
                'range_config': diagnostic_config.get('range', None),
                'weighted': diagnostic_config.get('weighted', True),
                'density': diagnostic_config.get('density', True),
                'box_brd': diagnostic_config.get('box_brd', True),
                'outputdir': outputdir,
                'rebuild': rebuild,
                'reader_kwargs': reader_kwargs,
                'save_pdf': save_pdf,
                'save_png': save_png,
                'dpi': dpi,
                'xlogscale': diagnostic_config.get('xlogscale', False),
                'ylogscale': diagnostic_config.get('ylogscale', True),
                'smooth': diagnostic_config.get('smooth', False),
                'smooth_window': diagnostic_config.get('smooth_window', 5),
                'xmin': diagnostic_config.get('xmin', None),
                'xmax': diagnostic_config.get('xmax', None),
                'ymin': diagnostic_config.get('ymin', None),
                'ymax': diagnostic_config.get('ymax', None),
                'loglevel': loglevel
            }

            # Process variables
            for var in diagnostic_config.get('variables', []):
                var_config, regions = load_var_config(config_dict, var)
                process_variable_or_formula(
                    var_config=var_config,
                    regions=regions,
                    formula=False,
                    **common_params
                )

            # Process formulae
            for var in diagnostic_config.get('formulae', []):
                var_config, regions = load_var_config(config_dict, var)
                process_variable_or_formula(
                    var_config=var_config,
                    regions=regions,
                    formula=True,
                    **common_params
                )

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)
    logger.info("Histogram diagnostic completed.")
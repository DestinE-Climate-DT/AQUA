#!/usr/bin/env python3
"""
Command-line interface for LatLonProfiles diagnostic.

This CLI allows to run the LatLonProfiles diagnostic for zonal or meridional profiles.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
"""

import sys
import argparse
from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.lat_lon_profiles.util_cli import load_var_config, process_variable_or_formula


def parse_arguments(args):
    """Parse command-line arguments for LatLonProfiles diagnostic.

    Args:
        args (list): list of command-line arguments to parse.

    Returns:
        argparse.Namespace: Parsed command-line arguments.
    """
    parser = argparse.ArgumentParser(description='LatLonProfiles CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='LatLonProfiles CLI')
    logger.info(f"Running LatLonProfiles diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='lat_lon_profiles', config=args.config,
                                         default_config='config_lat_lon_profiles.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)
    logger.info(f"Regrid option is set to {regrid}")
    realization = get_arg(args, 'realization', None)
    # This reader_kwargs will be used if the dataset corresponding value is None or not present
    if realization:
        reader_kwargs = {'realization': realization}
    else:
        reader_kwargs = config_dict['datasets'][0].get('reader_kwargs') or {}

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)
    create_catalog_entry = config_dict['output'].get('create_catalog_entry', True)

    # LatLonProfiles diagnostic
    if 'lat_lon_profiles' in config_dict['diagnostics']:
        if config_dict['diagnostics']['lat_lon_profiles']['run']:
            logger.info("LatLonProfiles diagnostic is enabled.")

            # Extract all configuration
            diagnostic_config = config_dict['diagnostics']['lat_lon_profiles']
            diagnostic_name = diagnostic_config.get('diagnostic_name', 'lat_lon_profiles')
            mean_type = diagnostic_config.get('mean_type', 'zonal')
            center_time = diagnostic_config.get('center_time', True)
            exclude_incomplete = diagnostic_config.get('exclude_incomplete', True)
            box_brd = diagnostic_config.get('box_brd', True)
            compute_std = diagnostic_config.get('compute_std', False)
            compute_seasonal = diagnostic_config.get('seasonal', True)
            compute_longterm = diagnostic_config.get('longterm', True)

            freq = []
            if compute_seasonal:
                freq.append('seasonal')
            if compute_longterm:
                freq.append('longterm')

            # Process variables
            for var in diagnostic_config.get('variables', []):
                var_config, regions = load_var_config(config_dict, var)
                process_variable_or_formula(
                    config_dict=config_dict,
                    var_config=var_config,
                    regions=regions,
                    datasets=config_dict['datasets'],
                    mean_type=mean_type,
                    diagnostic_name=diagnostic_name,
                    regrid=regrid,
                    freq=freq,
                    compute_std=compute_std,
                    exclude_incomplete=exclude_incomplete,
                    center_time=center_time,
                    box_brd=box_brd,
                    outputdir=outputdir,
                    rebuild=rebuild,
                    reader_kwargs=reader_kwargs,
                    save_pdf=save_pdf,
                    save_png=save_png,
                    dpi=dpi,
                    compute_longterm=compute_longterm,
                    compute_seasonal=compute_seasonal,
                    loglevel=loglevel,
                    formula=False  # <-- Variable
                )

            # Process formulae
            for var in diagnostic_config.get('formulae', []):
                var_config, regions = load_var_config(config_dict, var)
                process_variable_or_formula(
                    config_dict=config_dict,
                    var_config=var_config,
                    regions=regions,
                    datasets=config_dict['datasets'],
                    mean_type=mean_type,
                    diagnostic_name=diagnostic_name,
                    regrid=regrid,
                    freq=freq,
                    compute_std=compute_std,
                    exclude_incomplete=exclude_incomplete,
                    center_time=center_time,
                    box_brd=box_brd,
                    outputdir=outputdir,
                    rebuild=rebuild,
                    reader_kwargs=reader_kwargs,
                    save_pdf=save_pdf,
                    save_png=save_png,
                    dpi=dpi,
                    compute_longterm=compute_longterm,
                    compute_seasonal=compute_seasonal,
                    loglevel=loglevel,
                    formula=True  # <-- Formulae
                )

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)
    logger.info("LatLonProfiles diagnostic completed.")
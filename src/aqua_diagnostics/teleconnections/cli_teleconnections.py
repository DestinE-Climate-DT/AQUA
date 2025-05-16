#!/usr/bin/env python3
'''
Command-line interface for Teleconnections diagnostic.

This CLI allows to run the NAO and ENSO diagnostics.
Details of the run are defined in a yaml configuration file for a
single or multiple experiments.
'''
import argparse
import sys

from aqua.logger import log_configure
from aqua.util import get_arg
from aqua.version import __version__ as aqua_version
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import load_diagnostic_config, merge_config_args
from aqua.diagnostics.teleconnections import NAO, ENSO
from aqua.diagnostics.teleconnections import PlotNAO, PlotENSO


def parse_arguments(args):
    """Parse command-line arguments for Teleconnections diagnostic.

    Args:
        args (list): list of command-line arguments to parse.
    """
    parser = argparse.ArgumentParser(description='Teleconnections CLI')
    parser = template_parse_arguments(parser)
    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='Teleconnections CLI')
    logger.info(f"Running Teleconnections diagnostic with AQUA version {aqua_version}")

    cluster = get_arg(args, 'cluster', None)
    nworkers = get_arg(args, 'nworkers', None)

    client, cluster, private_cluster, = open_cluster(nworkers=nworkers, cluster=cluster, loglevel=loglevel)

    # Load the configuration file and then merge it with the command-line arguments,
    # overwriting the configuration file values with the command-line arguments.
    config_dict = load_diagnostic_config(diagnostic='teleconnections', args=args,
                                         default_config='config_teleconnections.yaml',
                                         loglevel=loglevel)
    config_dict = merge_config_args(config=config_dict, args=args, loglevel=loglevel)

    regrid = get_arg(args, 'regrid', None)

    # Output options
    outputdir = config_dict['output'].get('outputdir', './')
    rebuild = config_dict['output'].get('rebuild', True)
    save_pdf = config_dict['output'].get('save_pdf', True)
    save_png = config_dict['output'].get('save_png', True)
    dpi = config_dict['output'].get('dpi', 300)

    if 'teleconnections' in config_dict['diagnostics']:
        if 'NAO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['NAO']['run']:
                logger.info('Running NAO teleconnections diagnostic')

                nao = [None] * len(config_dict['datasets'])

                for i, dataset in enumerate(config_dict['datasets']):
                    logger.info(f'Running dataset: {dataset}')

        if 'ENSO' in config_dict['diagnostics']['teleconnections']:
            if config_dict['diagnostics']['teleconnections']['ENSO']['run']:
                logger.info('Running ENSO teleconnections diagnostic')

                enso = [None] * len(config_dict['datasets'])

                for i, dataset in enumerate(config_dict['datasets']):
                    logger.info(f'Running dataset: {dataset}')

    close_cluster(client=client, cluster=cluster, private_cluster=private_cluster, loglevel=loglevel)

    logger.info('Teleconnections diagnostic finished.')

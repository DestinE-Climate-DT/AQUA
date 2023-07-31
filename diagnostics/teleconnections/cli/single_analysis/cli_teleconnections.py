#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA teleconnections command line interface for a single dataset.
Reads configuration file and performs teleconnections diagnostic.
'''
import sys
import argparse

from aqua.util import load_yaml, get_arg
from teleconnections.plots import single_map_plot
from teleconnections.tc_class import Teleconnection
from teleconnections.tools import get_dataset_config


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='Teleconnections CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-d', '--definitive', action='store_true',
                        required=False,
                        help='if True, files are saved, default is False')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


if __name__ == '__main__':

    print('Running teleconnections diagnostic...')
    args = parse_arguments(sys.argv[1:])

    # Read configuration file
    file = get_arg(args, 'config', 'teleconnections_config.yaml')
    print('Reading configuration yaml file..')
    config = load_yaml(file)

    loglevel = get_arg(args, 'loglevel', 'WARNING')
    telecname = config['telecname']

    savefig = get_arg(args, 'definitive', False)
    savefile = get_arg(args, 'definitive', False)

    try:
        configdir = config['configdir']
    except KeyError:
        configdir = None

    # Get dataset configuration parameters
    # Search for entries under 'sources' key
    config_dict = get_dataset_config(sources=config,
                                     dataset_source='source')

    teleconnection = Teleconnection(telecname=telecname, configdir=configdir,
                                    **config_dict,  # from cli config file
                                    savefig=savefig, savefile=savefile,
                                    loglevel=loglevel)

    teleconnection.retrieve()
    teleconnection.evaluate_index()
    teleconnection.evaluate_correlation()
    teleconnection.evaluate_regression()

    if savefig:
        teleconnection.plot_index()
        single_map_plot(map=teleconnection.regression, loglevel=loglevel,
                        outputdir=teleconnection.outputfig,
                        filename=teleconnection.filename + '_regression.pdf',
                        save=True, cbar_label=teleconnection.var)
        single_map_plot(map=teleconnection.correlation, loglevel=loglevel,
                        outputdir=teleconnection.outputfig,
                        filename=teleconnection.filename + '_correlation.pdf',
                        save=True, cbar_label='Pearson correlation')

    print('Teleconnections diagnostic test run completed.')

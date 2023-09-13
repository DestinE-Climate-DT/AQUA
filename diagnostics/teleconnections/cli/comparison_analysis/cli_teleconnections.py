#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA teleconnections command line interface. Reads configuration file and
performs teleconnections diagnostic.
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

    parser = argparse.ArgumentParser(description='Teleconnections comparison CLI')
    parser.add_argument('-c', '--config', type=str,
                        required=False,
                        help='yaml configuration file')
    parser.add_argument('-d', '--definitive', action='store_true',
                        required=False,
                        help='if True, files are saved, default is False')
    parser.add_argument('-l', '--loglevel', type=str,
                        required=False,
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
    sources = config['sources']
    common_outputfig = config['common_outputfig']

    config_dict = []

    for dataset_source in sources:
        if loglevel == 'DEBUG':
            print('Obtaining config for dataset_source: ', dataset_source)

        config_dict.append(get_dataset_config(sources=sources,
                                              dataset_source=dataset_source))

    # Get observational dataset configuration parameters
    # Search for entries under 'obs' key
    obs_dict = config['obs']

    teleconnections = []

    # Initialize Teleconnection class
    for config in config_dict:
        if loglevel == 'DEBUG':
            print('Initializing Teleconnection class for dataset_source: ',
                  config['model'], config['exp'], config['source'])
        teleconnections.append(Teleconnection(telecname=telecname, **config,
                                              savefig=savefig,
                                              savefile=savefile,
                                              configdir=configdir,
                                              loglevel=loglevel))

    # Retrieve data, evaluate teleconnection index, correlation and regression
    for teleconnection in teleconnections:
        if loglevel == 'DEBUG':
            print('Retrieving data for dataset_source: ',
                  teleconnection.model, teleconnection.exp,
                  teleconnection.source)
        teleconnection.run()

    # Initialize Teleconnection class for observational dataset
    teleconnection_obs = Teleconnection(telecname=telecname, **obs_dict,
                                        savefig=savefig, savefile=savefile,
                                        configdir=configdir, loglevel=loglevel)
    teleconnection_obs.run()

    if savefig:
        if loglevel == 'INFO' or loglevel == 'DEBUG':
            print('Saving figures...')

        # Set colorbar label
        if telecname == 'NAO':
            cbar_label = 'msl [hPa]'
        elif telecname == 'ENSO':
            cbar_label = 'sst [K]'
        elif telecname == 'ENSO_2t':
            cbar_label = '2t [K]'
        else:
            cbar_label = None

        for teleconnection in teleconnections:
            # Index
            teleconnection.plot_index()

            # Regression
            filename = 'teleconnections_' + teleconnection.model + '_' + teleconnection.exp + '_' + teleconnection.source + '_'
            filename = filename + telecname + '_regression.pdf'
            title = telecname + ' regression map' + ' (' + teleconnection.model + ', ' + teleconnection.exp + ')'
            single_map_plot(map=teleconnection.regression, loglevel=loglevel,
                            save=True, outputdir=teleconnection.outputfig,
                            filename=filename, title=title, cbar_label=cbar_label)

            # Correlation
            filename = 'teleconnections_' + teleconnection.model + '_' + teleconnection.exp + '_' + teleconnection.source + '_'
            filename = filename + telecname + '_correlation.pdf'
            title = telecname + ' correlation map' + ' (' + teleconnection.model + ', ' + teleconnection.exp + ')'
            single_map_plot(map=teleconnection.correlation, loglevel=loglevel,
                            save=True, outputdir=teleconnection.outputfig,
                            filename=filename, title=title,
                            cbar_label='Pearson correlation coefficient')

        teleconnection_obs.plot_index()

        # Regression
        filename = 'teleconnections_' + teleconnection_obs.model + '_' + teleconnection_obs.exp + '_' + teleconnection_obs.source + '_'
        filename = filename + telecname + '_regression.pdf'
        title = telecname + ' regression map' + ' (' + teleconnection_obs.model + ', ' + teleconnection_obs.exp + ')'
        single_map_plot(map=teleconnection_obs.regression, loglevel=loglevel,
                        save=True, outputdir=teleconnection_obs.outputfig,
                        filename=filename, title=title)

        # Correlation
        filename = 'teleconnections_' + teleconnection_obs.model + '_' + teleconnection_obs.exp + '_' + teleconnection_obs.source + '_'
        filename = filename + telecname + '_correlation.pdf'
        title = telecname + ' correlation map' + ' (' + teleconnection_obs.model + ', ' + teleconnection_obs.exp + ')'
        single_map_plot(map=teleconnection_obs.correlation, loglevel=loglevel,
                        save=True, outputdir=teleconnection_obs.outputfig,
                        filename=filename, title=title)

    print('Teleconnections diagnostic test run completed.')

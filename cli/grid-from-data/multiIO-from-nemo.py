#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line tool to create an healpix grid from an oceanic file
"""
import argparse
import sys
from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.logger import log_configure


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA gribber')
    parser.add_argument('-c', '--config', type=str, required=False,
                        help='yaml file with exp information and directories')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='multiIO-from-nemo')

    file = get_arg(args, 'config', 'config-multiIO-nemo.yaml')
    logger.info('Reading configuration from %s', file)
    config = load_yaml(file)

    # Configuration needed to retrieve the data
    model = config['model']
    exp = config['exp']
    source = config['source']
    var = config['var']
    logger.info('Retrieving %s from %s %s %s', var, model, exp, source)

    # Configuration needed to save the file
    tgt = config['tgt']
    model_name = config['model_name']
    model3d = config['3d']

    # Create Reader object
    reader = Reader(model=model, exp=exp, source=source,
                    areas=False, fix=False, loglevel=loglevel)
    data = reader.retrieve(var=var)

    # Save data in a netcdf file
    create_folder(tgt)
    if model3d:
        grid_name = 'nemo-multiIO-r025-3d'
    else:
        grid_name = 'nemo-multiIO-r025'
    filename = tgt + '/' + grid_name + '.nc'

    logger.info('Saving data in %s', filename)
    data[var].isel(time=0).to_netcdf(filename, format='NETCDF4', engine='netcdf4',
                                     encoding={var: {'zlib': True, 'complevel': 9}})

    logger.info('Done')

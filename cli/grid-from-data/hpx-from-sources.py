#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line tool to create an healpix grid from an oceanic file
"""
import argparse
import sys
import os
from cdo import Cdo
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
    logger = log_configure(log_level=loglevel, log_name='hpx-from-nemo')

    file = get_arg(args, 'config', 'config-hpx-fesom.yaml')
    logger.info('Reading configuration from %s', file)
    config = load_yaml(file)

    # Configuration needed to retrieve the data
    model = config['model']
    exp = config['exp']
    source = config['source']
    var = config['var']
    logger.info('Retrieving %s from %s %s %s', var, model, exp, source)

    # Configuration needed to save the file
    tmp = config['tmp']
    model_name = config['model_name']
    zoom = config['zoom']
    model3d = config['3d']
    nested = config['nested']

    # Create Reader object
    reader = Reader(model=model, exp=exp, source=source,
                    areas=False, fix=False, loglevel=loglevel)
    data = reader.retrieve(var=var)

    if model3d:
        logger.debug("Modifying level axis attributes as Z")
        data['level'].attrs['axis'] = 'Z'

    # Save data in a netcdf file on the temporary directory
    create_folder(tmp)
    filename = tmp + '/' + model + '_' + exp + '_' + source + '_' + var + '.nc'

    logger.info('Saving data in %s', filename)
    data[var].isel(time=0).to_netcdf(filename)

    # CDO command to set the grid
    cdo = Cdo()

    # cdo command setup:
    nside = 2**zoom
    if nested:
        grid_name = 'hp' + str(nside) + '_nested'
    else:  # ring
        grid_name = 'hp' + str(nside) + '_ring'

    # setting output filename
    tgt = config['tgt']
    create_folder(tgt)
    filename_tgt = tgt + '/' + model_name + '_hpz' + str(zoom)
    if nested:
        filename_tgt = filename_tgt + '_nested_oce'
    else:
        filename_tgt = filename_tgt + '_ring_oce'

    if model3d:
        filename_tgt = filename_tgt + '_level.nc'
    else:
        filename_tgt = filename_tgt + '.nc'

    logger.info('Setting grid %s', grid_name)
    logger.info('Saving data in %s', filename_tgt)
    cdo.setgrid(grid_name, input=filename, output=filename_tgt, options="-f nc4 -z zip")

    logger.info('Removing temporary file %s', filename)
    os.remove(filename)

    logger.info('Done')

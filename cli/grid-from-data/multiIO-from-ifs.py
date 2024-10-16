#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
AQUA command line tool to create an multio grid file from IFS
"""
import argparse
import sys
import os
from aqua import Reader
from aqua.util import load_yaml, get_arg, create_folder
from aqua.logger import log_configure
from aqua.util import ConfigPath, load_yaml
from cdo import *   # python version
cdo = Cdo()

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

    file = get_arg(args, 'config', 'config-multiIO-ifs.yaml')
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
    create_folder(tgt)
    model_name = config['model_name']
    resolution = config['multio_resolution']

    # Create Reader object
    reader = Reader(model=model, exp=exp, source=source,
                    areas=False, fix=False, loglevel=loglevel)
    data = reader.retrieve(var=var)
    
    configdir = ConfigPath().get_config_dir()
    grid_definition = load_yaml(os.path.join(configdir, 'grids', 'default.yaml'))

    # multio grids are staggered, adding a final if the grid has not it
    if not resolution.endswith('s'):
        resolution = resolution + 's'
    cdo_grid = grid_definition['grids'][resolution]
    logger.info('CDO grid defined as %s', cdo_grid)

    GRID_NAME = model_name + '-multiIO-' + resolution
    temp_file = os.path.join(tgt, 'temp_unstructured.nc') 
    grid_file = os.path.join(tgt, GRID_NAME + '.nc')
    area_file = os.path.join(tgt, 'cell_area_' + GRID_NAME + '.nc')

    logger.info('Saving data in %s', temp_file)
    data[var].isel(time=0).to_netcdf(temp_file, engine='netcdf4')

    logger.info('Setting grid file in %s', grid_file)
    cdo.invertlat(input=f'-setgrid,{cdo_grid} {temp_file}', output = grid_file)
    logger.info('Setting area file in %s', area_file)
    cdo.setgrid(temp_file, input=f'-gridarea {grid_file}', output = area_file)

    os.remove(temp_file)
    
    logger.info('Done')

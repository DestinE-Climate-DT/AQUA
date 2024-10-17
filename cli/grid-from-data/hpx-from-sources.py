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

reasonable_vert_coords = ['depth_full', 'depth_half', 'level']


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA grids from data')
    parser.add_argument('-c', '--config', type=str, required=True,
                        help='yaml file with exp information and directories')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument('-z', '--zoom', type=str,
                        help='override zoom: convenient for loop on many zoom levels')

    return parser.parse_args(args)


if __name__ == '__main__':
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_level=loglevel, log_name='hpx-from-sources')

    file = get_arg(args, 'config', None)
    
    logger.info('Reading configuration from %s', file)
    config = load_yaml(file)

    # Configuration needed to retrieve the data
    model = config['model']
    exp = config['exp']
    source = config['source']
    var = config['var']
    resolution = config.get('resolution')
    extra = config.get('extra')
    logger.info('Retrieving %s from %s %s %s', var, model, exp, source)

    # Configuration needed to save the file
    tmp = config['tmp']
    model_name = config['model_name']
    zoom = int(get_arg(args, 'zoom', config.get('zoom'))) #HACK for zoom
    catalog = config.get('catalog')
    nested = config['nested']

    # adding zoom if found
    mykwargs = {}
    if zoom:
        mykwargs = {**mykwargs, **{'zoom': zoom}}
    if catalog:
        mykwargs = {**mykwargs, **{'catalog': catalog}}
        

    # Create Reader object
    reader = Reader(model=model, exp=exp, source=source,
                    areas=False, fix=False, loglevel=loglevel, **mykwargs)
    data = reader.retrieve(var=var)

    # detect vertical coordinate from a set of plausible values
    vert_coord = list(set(reasonable_vert_coords) & set(data.coords))

    #automatic detection of 3d
    if vert_coord:
        model3d = True
        if len(vert_coord)>1:
            raise KeyError("Too many vertical coordinates identified, check the data manually")
        vert_coord = vert_coord[0]
    else: 
        model3d = False

    if model3d:
        logger.debug("Modifying level axis attributes as Z")
        data[vert_coord].attrs['axis'] = 'Z'

    # Save data in a netcdf file on the temporary directory
    create_folder(tmp)

    filename = tmp + '/' + model + '_' + exp + '_' + source + '_' + var + '.nc'

    logger.info('Saving data in %s', filename)
    data[var].isel(time=0).to_netcdf(filename)

    # CDO command to set the grid
    cdo = Cdo()

    # cdo command setup:
    nside = 2**zoom
   
    # Setting grid name
    if nested:
        grid_name = f'hp{nside}_nested'
    else:  # ring
        grid_name = f'hp{nside}_ring'

    # Setting output filename
    tgt = config['tgt']
    create_folder(tgt)
    filename_tgt = os.path.join(tgt, model_name)

    # add info on original data resolution if available
    if resolution:
        filename_tgt = f"{filename_tgt}-{resolution}"

    # add info on hpz zoom
    filename_tgt = f"{filename_tgt}_hpz{zoom}"

    # options for nested and ringed
    if nested:
        filename_tgt = f"{filename_tgt}_nested_oce"
    else:
        filename_tgt = f"{filename_tgt}_ring_oce"

    if model3d:
        filename_tgt = f"{filename_tgt}_{vert_coord}"

    if extra:
        filename_tgt = f"{filename_tgt}_{extra}"

    filename_tgt = f'{filename_tgt}.nc'

    logger.info('Setting grid %s', grid_name)
    logger.info('Saving data in %s', filename_tgt)
    cdo.setgrid(grid_name, input=filename, output=filename_tgt, options="-f nc4 -z zip")

    logger.info('Removing temporary file %s', filename)
    os.remove(filename)

    logger.info('Done')

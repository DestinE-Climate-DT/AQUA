"""Utility module for LRA/OPA"""

import os
from aqua.util import dump_yaml, load_yaml
from aqua.util import ConfigPath
from aqua.logger import log_configure


def opa_catalog_entry(datadir, model, exp, source, frequency='monthly',
                      loglevel='WARNING'):
    """
    Create an entry in the AQUA catalog based on the presence of output from OPA in datadir
    to be used by the LRA generator in both source and regrid yaml

    Args:
        datadir (str): path to the directory containing the data
        model (str): name of the model
        exp (str): name of the experiment
        source (str): name of the origin source
        frequency (str, opt): frequency of the data, default is 'monthly'
        loglevel (str, opt): logging level, default is 'WARNING'
    """
    logger = log_configure(log_level=loglevel, log_name='opa_catalog_entry')

    entry_name = 'opa'
    logger.info('Creating catalog entry %s %s %s', model, exp, entry_name)

    # load the catalog experiment file
    Configurer = ConfigPath()
    configdir = Configurer.configdir
    machine = Configurer.machine

    # find the catalog of my experiment
    catalogfile = os.path.join(configdir, 'machines', machine,
                               'catalog', model, exp + '.yaml')

    # load, add the block and close
    cat_file = load_yaml(catalogfile)

    # read the grid info from the origin source
    if source in cat_file['sources']:
        try:
            grid = cat_file['sources'][source]['metadata']['source_grid_name']
        except KeyError:
            logger.error('Cannot find source grid name in catalog, assuming lon-lat')
            grid = 'lon-lat'
    else:
        raise KeyError('Cannot find source %s in catalog' % source)

    # define the block to be uploaded into the catalog
    description = 'OPA output from %s' % source

    block_cat = {
        'driver': 'netcdf',
        'args': {
            'urlpath': os.path.join(datadir, f'*{frequency}_mean.nc'),
            'chunks': {},
            'xarray_kwargs': {
                'decode_times': True
            }
        },
        'description': description,
        'metadata': {
            'source_grid_name': grid,
        }
    }

    # if zoom:
    #     block_zoom = {
    #         'parameters': {
    #             'zoom': {
    #                 'allowed': [zoom],
    #                 'default': zoom,
    #                 'description': 'zoom resolution of the dataset',
    #                 'type': 'int'
    #             }
    #         }
    #     }
    #     block_cat.update(block_zoom)

    # add the block to the catalog
    cat_file['sources'][entry_name] = block_cat
    dump_yaml(outfile=catalogfile, cfg=cat_file)

    return entry_name

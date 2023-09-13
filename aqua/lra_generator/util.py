"""Utility module for LRA/OPA"""

import os
import copy
import logging
from aqua.util import dump_yaml, load_yaml
from aqua.util import get_config_dir, get_machine


def opa_catalog_entry(datadir, model, exp, source, frequency='monthly'):
    """
    Create an entry in the AQUA catalog based on the presence of output from OPA in datadir
    to be used by the LRA generator in both source and regrid yaml
    """

    entry_name=f'opa-{frequency}'
    logging.warning('Creating catalog entry %s %s %s', model, exp, entry_name)

    # define the block to be uploaded into the catalog
    block_cat = {
        'driver': 'netcdf',
        'args': {
            'urlpath': os.path.join(datadir, f'*{frequency}_mean.nc'),
            'chunks': {},
            'xarray_kwargs': {
                'decode_times': True
            }
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

    configdir = get_config_dir()
    machine = get_machine(configdir)

    # find the catalog of my experiment
    catalogfile = os.path.join(configdir, 'machines', machine,
                                'catalog', model, exp+'.yaml')

    # load, add the block and close
    cat_file = load_yaml(catalogfile)
    cat_file['sources'][entry_name] = block_cat
    dump_yaml(outfile=catalogfile, cfg=cat_file)

    # find the regrid of my experiment
    regridfile = os.path.join(configdir, 'machines', machine,
                                'regrid.yaml')
    cat_file = load_yaml(regridfile)
    dictexp = cat_file['source_grids'][model][exp]
    if source in dictexp:
        regrid_entry = dictexp[entry_name]
    elif 'default' in dictexp:
        logging.warning('No entry found for source %s, assuming the default', source)
        regrid_entry = dictexp['default']
    else:
        raise KeyError('Cannot find experiment information regrid file')

    cat_file['source_grids'][model][exp][entry_name] = copy.deepcopy(regrid_entry)

    dump_yaml(outfile=regridfile, cfg=cat_file)

    return entry_name
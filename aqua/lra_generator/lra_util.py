"""Utility module for LRA/OPA"""

import os
import shutil
from aqua.util import dump_yaml, load_yaml
from aqua.util import ConfigPath
from aqua.logger import log_configure

def opa_catalog_entry(datadir, model, exp, source, catalog=None,
                      fixer_name=False, frequency='monthly',
                      loglevel='WARNING'):
    """
    Create an entry in the AQUA catalog based on the presence of output from OPA in datadir
    to be used by the LRA generator in both source and regrid yaml

    Args:
        datadir (str): path to the directory containing the data
        catalog (str): name of the catalog, None for getting it automatically
        model (str): name of the model
        exp (str): name of the experiment
        source (str): name of the origin source
        fixer_name (str): fix to be used when reading the opa. Default is False
        frequency (str, opt): frequency of the data, default is 'monthly'
        loglevel (str, opt): logging level, default is 'WARNING'

    Returns:
        entry_name (str): name of the entry created in the catalog

    Raises:
        KeyError: if the origin source is not found in the catalog
    """
    logger = log_configure(log_level=loglevel, log_name='opa_catalog_entry')

    entry_name = 'opa'
    entry_name += '_%s' % source
    logger.info('Creating catalog entry %s %s %s', model, exp, entry_name)

    # load the catalog experiment file
    Configurer = ConfigPath()
    configdir = Configurer.configdir
    if catalog is None:
        catalog = Configurer.catalog

    # find the catalog of my experiment
    catalogfile = os.path.join(configdir, 'catalogs', catalog,
                               'catalog', model, exp + '.yaml')

    # load, add the block and close
    cat_file = load_yaml(catalogfile)

    # NOTE: commented because the correct grid is lon-lat when generated by OPA
    #       see issue #691
    # # read the grid info from the origin source
    # if source in cat_file['sources']:
    #     try:
    #         grid = cat_file['sources'][source]['metadata']['source_grid_name']
    #     except KeyError:
    #         logger.error('Cannot find source grid name in catalog, assuming lon-lat')
    #         grid = 'lon-lat'
    # else:
    #     raise KeyError('Cannot find source %s in catalog' % source)

    # define the block to be uploaded into the catalog
    description = 'OPA output from %s as origin source' % source

    block_cat = {
        'driver': 'netcdf',
        'args': {
            'urlpath': os.path.join(datadir, f'*{frequency}_mean.nc'),
            'chunks': {},
            'xarray_kwargs': {
                'decode_times': True,
                'combine': 'by_coords'
            }
        },
        'description': description,
        'metadata': {
            'source_grid_name': 'lon-lat',
            'fixer_name': fixer_name
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

# NO LONGER NECESSARY
# def check_correct_ifs_fluxes(xfield, threshold=100, loglevel='WARNING'):

#     """
#     Giving a Xarray DataArray,
#     check if the first time step is more than 100 times larger
#     This is done to protect LRA from wrong fluxes produced by IFS for every new month
#     """

#     logger = log_configure(log_level=loglevel, log_name='check_ifs_fluxes')

#     data1 = xfield.isel(time=0).mean().values
#     data2 = xfield.isel(time=1).mean().values
#     ratio = abs(data1)/abs(data2)
#     logger.info('Ratio of first two timesteps is %s', round(ratio,2))
#     if ratio > threshold:
#         logger.warning('Ratio %s is unrealistically high, we will set the first time step to NaN', round(ratio, 2))
#         xfield.loc[{'time': xfield.time.values[0]}] = np.nan

#    return xfield

def move_tmp_files(tmp_directory, output_directory):
    """
    Move temporary NetCDF files from the tmp directory to the output directory,
    changing their name by removing "_tmp" suffix. 
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for tmp_file in os.listdir(tmp_directory):
        if tmp_file.endswith(".nc"):
            if "_tmp" in tmp_file:
                new_file_name = tmp_file.replace("_tmp", "")
            else:
                new_file_name = tmp_file
            tmp_file_path = os.path.join(tmp_directory, tmp_file)
            new_file_path = os.path.join(output_directory, new_file_name)
            shutil.move(tmp_file_path, new_file_path)

def replace_intake_vars(path, catalog=None):
        
        """
        Replace the intake jinja vars into a string for a predefined catalog

        Args:
            catalog:  the catalog name where the intake vars must be read
            path: the original path that you want to update with the intake variables
        """

            # we exploit of configurerto get info on intake_vars so that we can replace them in the urlpath
        Configurer = ConfigPath(catalog=catalog)
        _, intake_vars = Configurer.get_machine_info()

        # loop on available intake_vars, replace them in the urlpath
        for name in intake_vars.keys():
            replacepath = intake_vars[name]
            if replacepath is not None and replacepath in path:
                # quotes used to ensure that then you can read the source
                path = path.replace(replacepath, "{{ " + name + " }}")
        
        return path


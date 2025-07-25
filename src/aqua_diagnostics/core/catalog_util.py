import os
from aqua.util import dump_yaml, load_yaml
from aqua.logger import log_configure
from aqua.util import ConfigPath
"""
for timeseries we need frequency mon/ann
for other diagnostics we don't need the frequency 
"""

class Catalog_util:
    
    def __init__(
        self,
        diagnostic_name=None,
        diagnostic_product=None,
        catalog=None,
        model=None,
        exp=None,
        realization=None,
        extra_keys=None,
        parts_dict=None,
        basedir=None,
    ):
        self.diagnotic_name = diagnostic_name
        self.diagnostic_product =  diagnostic_product
        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.realization = realization
        self.extra_keys = extra_keys
        self.parts_dict = parts_dict
        self.basedir = basedir

    def create_catalog_entry(self):
        """
        Create an entry in the catalog
        """

        configpath = ConfigPath(catalog=self.catalog)
        configdir = configpath.configdir
        # find the catalog of the experiment and load it
        catalogfile = os.path.join(self.configdir, 'catalogs', self.catalog, 'catalog', self.model, self.exp + '.yaml')
        cat_file = load_yaml(catalogfile)

        frequency = extra_keys['freq'] if 'freq' in extra_keys else None
        # if extra key has mon/ann then use frequency
        if frequency is not None:
            entry_name = f'aqua-{self.diagnostic_name}-{self.diagnostic_product}-{frequency}'
        else:
            entry_name = f'aqua-{self.diagnostic_name}-{self.diagnostic_product}'

        self.logger.info('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)
        # source_grid_name 
        #sgn = self._define_source_grid_name()
        sgn = "null" # null for now

        if entry_name in cat_file['sources']:
            catblock = cat_file['sources'][entry_name]
        else:
            catblock = None
        
        # define the entry details (need a function)
        block = self.create_entry_details(
            basedir=self.basedir, catblock=catblock, source_grid_name=sgn
        )

        cat_file['sources'][entry_name] = block

        # dump the update file
        dump_yaml(outfile=catalogfile, cfg=cat_file)


    def create_entry_details(self, basedir=None, catblock=None, driver='netcdf', source_grid_name='null'):
        """
        Create an entry in the catalog for the LRA

        Args:
            basedir (str): Base directory for the output files.
            catblock (dict, optional): Existing catalog block to update. Defaults to None if not existing.
            driver (str): Driver type for the catalog entry. Defaults to 'netcdf', alternative is 'zarr'.
            source_grid_name (str): Name of the source grid. Defaults to 'lon-lat'. Can be AQUA grid, or 'False' if not applicable.

        Returns:
            dict: The catalog block with the updated urlpath and metadata.
        """

        ###### This is addition
        if self.extra_keys:
            self.parts_dict.update({key: "*" for key in self.extra_keys})
        
        # Remove None values
        parts = [str(value) for value in self.parts_dict.values() if value is not None]

        # Join all parts
        filename = '.'.join(parts) + ".nc"

        _parts = [self.catalog, self.model, self.exp, self.realization]
        folder = os.path.join(*[p for p in _parts if p])
        urlpath = os.path.join(self.basedir, folder, filename)
        ###################

        self.logger.info('Fully expanded urlpath %s', urlpath)

        urlpath = replace_intake_vars(catalog=self.catalog, path=urlpath)
        self.logger.info('New urlpath with intake variables is %s', urlpath)

        if catblock is None:
            # if the entry is not there, define the block to be uploaded into the catalog
            catblock = {
                'driver': driver,
                'description': f'AQUA {driver} data',
                'args': {
                    'urlpath': urlpath,
                    'chunks': {},
                },
                'metadata': {
                    'source_grid_name': source_grid_name,
                }
            }
        else:
            # if the entry is there, we just update the urlpath
            catblock['args']['urlpath'] = urlpath

        if driver == 'netcdf':
            catblock['args']['xarray_kwargs'] = {
                'decode_times': True,
                'combine': 'by_coords'
            }

            # TODO: add kwargs in form of key-value pairs to be added to the intake jinja strings
            
            ### working on this part
            catblock = self.replace_urlpath_jinja(catblock, self.realization, 'realization')
            region = extra_keys['region'] if 'region' in extra_keys else None
            if region is not None:
                catblock = self.replace_urlpath_jinja(catblock, region, 'region')

            stat = extra_keys['stat'] if 'stat' in extra_keys else None
            if stat is not None:
                catblock = self.replace_urlpath_jinja(catblock, stat, 'stat')

        return catblock

    #def _define_source_grid_name(self):
    #    """"
    #    Define the source grid name based on the resolution
    #    """
    #    if self.resolution in self.default_grids:
    #        return 'lon-lat'
    #    if self.resolution == 'native':
    #        try:
    #            return self.reader.source_grid_name
    #        except AttributeError:
    #            self.logger.warning('No source grid name defined in the reader, using resolution as source grid name')
    #            return False
    #    return self.resolution

    @staticmethod
    def replace_urlpath_jinja(block, value, name):
        """
        Replace the urlpath in the catalog entry with the given jinja parameter and
        add the parameter to the parameters block

        Args:
            block (dict): The catalog entry generated by `catalog_entry_details' to be updated
            value (str): The value to replace in the urlpath (e.g., 'r1', 'global', 'mean')
            name (str): The name of the parameter to add to the parameters block
                        and to be used in the urlpath (e.g., 'realization', 'region', 'stat')
        """
        if not value:
            return block
        # this loop is a bit tricky but is made to ensure that the right value is replaced
        for character in ['_', '/']:
            block['args']['urlpath'] = block['args']['urlpath'].replace(
                character + value + character, character + "{{" + name + "}}" + character)
        if 'parameters' not in block:
            block['parameters'] = {}
        if name not in block['parameters']:
            block['parameters'][name] = {}
            block['parameters'][name]['description'] = f"Parameter {name} for the LRA"
            block['parameters'][name]['default'] = value
            block['parameters'][name]['type'] = 'str'
            block['parameters'][name]['allowed'] = [value]
        else:
            if value not in block['parameters'][name]['allowed']:
                block['parameters'][name]['allowed'].append(value)

        return block

    @staticmethod
    def get_urlpath(block):
        """
        Get the urlpath for the catalog entry
        """
        return block['args']['urlpath']

    @staticmethod
    def replace_intake_vars(path, catalog=None):
        """
        Replace the intake jinja vars into a string for a predefined catalog

        Args:
            catalog:  the catalog name where the intake vars must be read
            path: the original path that you want to update with the intake variables
        """

        # We exploit of configurerto get info on intake_vars so that we can replace them in the urlpath
        Configurer = ConfigPath(catalog=catalog)
        _, intake_vars = Configurer.get_machine_info()

        # loop on available intake_vars, replace them in the urlpath
        for name in intake_vars.keys():
            replacepath = intake_vars[name]
            if replacepath is not None and replacepath in path:
                # quotes used to ensure that then you can read the source
                path = path.replace(replacepath, "{{ " + name + " }}")

        return path

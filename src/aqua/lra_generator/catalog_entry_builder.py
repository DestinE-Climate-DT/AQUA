"""Class to create a catalog entry for the LRA"""

from aqua.logger import log_configure
from .output_path_builder import OutputPathBuilder


class CatalogEntryBuilder():
    """Class to create a catalog entry for the LRA"""

    def __init__(self, catalog, model, exp, var, realization='r1',
                 resolution=None, frequency=None, stat="mean",
                 region="global", level=None, loglevel='WARNING', **kwargs):

        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.var = var
        self.realization = realization
        self.resolution = resolution
        self.frequency = frequency
        self.stat = stat
        self.region = region
        self.level = level
        self.kwargs = kwargs
        self.ouput_path_builder = OutputPathBuilder(catalog=catalog, model=model, exp=exp, var=var,
                                                    realization=realization, resolution=self.resolution,
                                                    frequency=self.frequency, stat=self.stat, region=self.region,
                                                    level=self.level, **self.kwargs)
        self.logger = log_configure(log_level=loglevel, log_name='CatalogEntryBuilder')
        self.loglevel = loglevel

    def set_from_reader(self, reader_obj):
        """Guess resolution and frequency from AQUA reader."""
        self.ouput_path_builder.set_from_reader(reader_obj)

    def create_entry_name(self):
        """
        Create an entry name for the LRA
        """

        entry_name = f'lra-{self.resolution}-{self.frequency}'
        self.logger.info('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)

        return entry_name

    def create_entry_details(self, basedir=None):
        """
        Create an entry in the catalog for the LRA
        """

        urlpath = self.ouput_path_builder.build_path(basedir, year="*")

        self.logger.info('Fully expanded urlpath %s', urlpath)
        # urlpath = replace_intake_vars(catalog=self.catalog, path=urlpath)
        self.logger.info('New urlpath with intake variables is %s', urlpath)

        # find the catalog of my experiment and load it
        # catalogfile = os.path.join(self.configdir, 'catalogs', self.catalog,
        #                        'catalog', self.model, self.exp + '.yaml')
        # cat_file = load_yaml(catalogfile)

        # if the entry already exists, update the urlpath if requested and return
        # if entry_name in cat_file['sources']:
        #    self.logger.info('Catalog entry for %s %s %s already exists', self.model, self.exp, entry_name)
        #    self.logger.info('Updating the urlpath to %s', urlpath)
        #    cat_file['sources'][entry_name]['args']['urlpath'] = urlpath

        # else:
        # if the entry is not there, define the block to be uploaded into the catalog
        block_cat = {
            'driver': 'netcdf',
            'description': f'AQUA LRA data {self.frequency} at {self.resolution}',
            'args': {
                'urlpath': urlpath,
                'chunks': {},
                'xarray_kwargs': {
                    'decode_times': True,
                    'combine': 'by_coords'
                },
            },
            'metadata': {
                'source_grid_name': 'lon-lat',
            }
        }
        block_cat = self.replace_urlpath_jinja(block_cat, self.realization, 'realization')
        block_cat = self.replace_urlpath_jinja(block_cat, self.region, 'region')
        block_cat = self.replace_urlpath_jinja(block_cat, self.stat, 'stat')

        return block_cat

        # cat_file['sources'][entry_name] = block_cat

    @staticmethod
    def replace_urlpath_jinja(block, value, name):
        """
        Replace the urlpath in the catalog entry with the given jinja parameter and
        add the parameter to the parameters block

        Args:
            block (dict): The catalog entry block to modify
            value (str): The value to replace in the urlpath
            name (str): The name of the parameter to add to the parameters block
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

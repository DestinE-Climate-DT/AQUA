"""Class to create a catalog entry for the LRA"""

from aqua.logger import log_configure
#from .lra_util import replace_intake_vars
from .output_path_builder import OutputPathBuilder

class CatalogEntryBuilder():
    """Class to create a catalog entry for the LRA"""

    def __init__(self, basedir, catalog, model, exp, var, realization='r1',
                 resolution=None, frequency=None, stat=None,
                 region=None, level=None, loglevel='WARNING', **kwargs):
        
        self.basedir = basedir
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

        entry_name = f'aqua-{self.resolution}-{self.frequency}-{self.stat}'
        self.logger.info('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)

        return entry_name

    def create_entry_details(self, urlpath):
        """
        Create an entry in the catalog for the LRA
        """

        urlpath = self.ouput_path_builder.build_path(self.basedir, year="*")

        self.logger.info('Fully expanded urlpath %s', urlpath)
        #urlpath = replace_intake_vars(catalog=self.catalog, path=urlpath)
        self.logger.info('New urlpath with intake variables is %s', urlpath)

        # find the catalog of my experiment and load it
        #catalogfile = os.path.join(self.configdir, 'catalogs', self.catalog,
        #                        'catalog', self.model, self.exp + '.yaml')
        #cat_file = load_yaml(catalogfile)

        # if the entry already exists, update the urlpath if requested and return
        #if entry_name in cat_file['sources']:
        #    self.logger.info('Catalog entry for %s %s %s already exists', self.model, self.exp, entry_name)
        #    self.logger.info('Updating the urlpath to %s', urlpath)
        #    cat_file['sources'][entry_name]['args']['urlpath'] = urlpath

        #else: 
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
        block_cat = self.replace_urlpath(block_cat, self.realization, 'realization')
        block_cat = self.replace_urlpath(block_cat, self.region, 'region')
    

        return block_cat

            #cat_file['sources'][entry_name] = block_cat

    @staticmethod
    def replace_urlpath(block, value, name):
        """
        Replace the urlpath in the catalog entry with the given name
        """
        block['args']['urlpath'] = block['args']['urlpath'].replace(value, "{{" + name + "}}")
        if not 'parameters' in block:
            block['parameters'] = {}
        block['parameters'][name] = {}
        block['parameters'][name]['description'] = f"Parameter {name} for the LRA"
        block['parameters'][name]['default'] = value

        return block



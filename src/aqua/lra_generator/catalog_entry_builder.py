"""Class to create a catalog entry for the LRA"""

from aqua.logger import log_configure
from .output_path_builder import OutputPathBuilder
from .lra_util import replace_intake_vars


class CatalogEntryBuilder():
    """Class to create a catalog entry for the LRA"""

    def __init__(self, catalog, model, exp, resolution,
                 realization=None, frequency=None, stat=None,
                 region=None, level=None, loglevel='WARNING', **kwargs):
        
        """
        Initialize the CatalogEntryBuilder with the necessary parameters.

        Args:
            catalog (str): Name of the catalog.
            model (str): Name of the model.
            exp (str): Name of the experiment.
            resolution (str): Resolution of the data.
            realization (str, optional): Realization name. Defaults to 'r1'.
            frequency (str, optional): Frequency of the data. Defaults to 'native'.
            stat (str, optional): Statistic type. Defaults to 'nostat'.
            region (str, optional): Region. Defaults to 'global'.
            level (str, optional): Level. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            **kwargs: Additional keyword arguments for flexibility.
        """

        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.resolution = resolution
        
        # Ensure realization is formatted correctly
        if realization and realization.isdigit():
            realization = f'r{realization}'
        
        # Set defaults if not provided
        self.realization = realization if realization is not None else 'r1'
        self.frequency = frequency if frequency is not None else 'native'
        self.stat = stat if stat is not None else 'nostat'
        self.region = region if region is not None else 'global'

        self.level = level
        self.kwargs = kwargs
        self.opt = OutputPathBuilder(catalog=catalog, model=model, exp=exp,
                                                    realization=realization, resolution=self.resolution,
                                                    frequency=self.frequency, stat=self.stat, region=self.region,
                                                    level=self.level, **self.kwargs)
        self.logger = log_configure(log_level=loglevel, log_name='CatalogEntryBuilder')
        self.loglevel = loglevel

    def create_entry_name(self):
        """
        Create an entry name for the LRA
        """

        entry_name = f'lra-{self.resolution}-{self.frequency}'
        self.logger.info('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)

        return entry_name

    def create_entry_details(self, basedir=None, catblock=None, driver='netcdf', source_grid_name='lon-lat'):
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

        urlpath = self.opt.build_path(basedir=basedir, var="*", year="*")
        self.logger.info('Fully expanded urlpath %s', urlpath)

        urlpath = replace_intake_vars(catalog=self.catalog, path=urlpath)
        self.logger.info('New urlpath with intake variables is %s', urlpath)

        if catblock is None:
            # if the entry is not there, define the block to be uploaded into the catalog
            catblock = {
                'driver': driver,
                'description': f'AQUA {driver} LRA data {self.frequency} at {self.resolution}',
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
            catblock = self.replace_urlpath_jinja(catblock, self.realization, 'realization')
            catblock = self.replace_urlpath_jinja(catblock, self.region, 'region')
            catblock = self.replace_urlpath_jinja(catblock, self.stat, 'stat')

        return catblock

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

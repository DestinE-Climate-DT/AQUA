"""Class to create a catalog entry for DROP"""

import pandas as pd
from aqua.core.logger import log_configure
from aqua.core.util import format_realization
from aqua.core.util import replace_intake_vars, replace_urlpath_jinja
from aqua.core.util import frequency_string_to_pandas
from .output_path_builder import OutputPathBuilder


# default grid name for DROP outputs, if not specified otherwise
DEFAULT_DROP_GRID = 'lon-lat-r100'  

class CatalogEntryBuilder():
    """Class to create a catalog entry for DROP"""

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

        # Set defaults if not provided
        self.realization = format_realization(realization) # ensure realization is formatted correctly
        self.frequency = frequency if frequency is not None else 'native'
        self.stat = stat if stat is not None else 'nostat'
        self.region = region if region is not None else 'global'

        self.level = level
        self.kwargs = kwargs
        self.opt = OutputPathBuilder(catalog=catalog, model=model, exp=exp,
                                     realization=self.realization, resolution=self.resolution,
                                     frequency=self.frequency, stat=self.stat, region=self.region,
                                     level=self.level, **self.kwargs)
        self.logger = log_configure(log_level=loglevel, log_name='CatalogEntryBuilder')
        self.loglevel = loglevel

    def create_entry_name(self):
        """
        Create an entry name for the DROP outputs
        """
        # Default LRA-100-monthly entry keeps the 'lra-' prefix
        if self.resolution == 'r100' and self.frequency == 'monthly':
            entry_name = f'lra-{self.resolution}-{self.frequency}'
        else:
            # All other entries drop the 'lra-' prefix
            entry_name = f'{self.resolution}-{self.frequency}'

        return entry_name

    # def update_urlpath(self, oldpath, newpath):
    #     """Update the urlpath in the catalog entry."""
    #     old = to_list(oldpath)
    #     new = to_list(newpath)

    #     for n in new:
    #         if n not in old:
    #             old.append(n)

    #     return old if len(old) > 1 else old[0]

    def define_optimal_chunks(self, baseyear=1990):
        """Define optimal chunking for DROP outputs.
        
        Args:
            baseyear (int): Base year to define time chunking. Defaults to 1990
            
        Returns:
            dict: Dictionary with optimal chunk sizes for 'time', 'lat', and 'lon'."""

        chunks = {}

        # guessing cases for rXXX and rXXXs resolutions: all other cases stay undefined
        if len(self.resolution) == 4 and self.resolution.startswith('r'):
            ref_value = int(self.resolution[1:])
            chunks.update({'lat': 18000 // ref_value, 'lon': 36000 // ref_value})
        if len(self.resolution) == 5 and self.resolution.endswith('s'):
            ref_value = int(self.resolution[1:-1])
            chunks.update({'lat': (18000 // ref_value) + 1, 'lon': (36000 // ref_value)})

        # self guessing ot time chunking based on frequency
        freq = frequency_string_to_pandas(self.frequency)
        if freq:
            rng = pd.date_range(
                f'{baseyear}-01-01', f'{baseyear+1}-01-01',
                freq=freq, 
                inclusive="left"
            )
            chunks.update({'time': len(rng)})

        return chunks

    def create_entry_details(self, basedir=None, catblock=None, 
                             driver='netcdf', 
                             source_grid_name=DEFAULT_DROP_GRID):
        """
        Create an entry in the catalog for DROP

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

        # define optimal chunks for DROP outputs
        chunks = self.define_optimal_chunks()

        if catblock is None:
            # if the entry is not there, define the block to be uploaded into the catalog
            catblock = {
                'driver': driver,
                'description': f'AQUA {driver} DROP-generated data {self.frequency} at {self.resolution}',
                'args': {
                    'urlpath': urlpath,
                    'chunks': chunks,
                },
                'metadata': {
                    'source_grid_name': source_grid_name,
                }
            }
        else:
            # if the entry is there, we just update the urlpath
            # catblock['args']['urlpath'] = self.update_urlpath(catblock['args']['urlpath'], urlpath)
            catblock['args']['urlpath'] = urlpath
            self.logger.info('Updated urlpath in existing catalog entry to %s', catblock['args']['urlpath'])

        if driver == 'netcdf':
            catblock['args']['xarray_kwargs'] = {
                'decode_times': True,
                'combine': 'by_coords'
            }

            # Jinja parameters to be replaced in the urlpath
            jinja_params = {
                'realization': self.realization,
                'region': self.region,
                'stat': self.stat
            }

            # Apply replacements
            for param_name, param_value in jinja_params.items():
                catblock = replace_urlpath_jinja(catblock, param_value, param_name)
                self.logger.debug("Urlpath after replacing %s: %s", param_name, catblock['args']['urlpath'])

            # ugly safecheck to ensure that urlpath is a list of unique entries if multiple
            # catblock['args']['urlpath'] = catblock['args']['urlpath'] if isinstance(catblock['args']['urlpath'], str) else list(set(catblock['args']['urlpath']))

        return catblock

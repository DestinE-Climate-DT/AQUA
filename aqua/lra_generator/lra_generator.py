"""
LRA class for glob
"""

import os
from time import time
import glob
import dask
import yaml
import xarray as xr
from dask.distributed import Client, LocalCluster, progress
from dask.diagnostics import ProgressBar
from aqua.logger import log_configure
from aqua.reader import Reader
from aqua.util import create_folder, generate_random_string, load_yaml
from aqua.util import get_config_dir, get_machine


class LRAgenerator():
    """
    Class to generate LRA data at required frequency/resolution
    """
    def __init__(self,
                 model=None, exp=None, source=None,
                 var=None, vars=None, configdir=None,
                 resolution=None, frequency=None, fix=True,
                 outdir=None, tmpdir=None, nproc=1,
                 loglevel=None, overwrite=False, definitive=False):
        """
        Initialize the LRA_Generator class

        Args:
            model (string):          The model name from the catalog
            exp (string):            The experiment name from the catalog
            source (string):         The sourceid name from the catalog
            var (str, list):         Variable(s) to be processed and archived
                                     in LRA,vars in a synonim
            resolution (string):     The target resolution for the LRA
            frequency (string,opt):  The target frequency for averaging the
                                     LRA, if no frequency is specified,
                                     no time average is performed
            fix (bool, opt):         True to fix the data, default is True
            outdir (string):         Where the LRA is
            tmpdir (string):         Where to store temporary files,
                                     default is None.
                                     Necessary for dask.distributed
            configdir (string):      Configuration directory where the catalog 
                                     are found
            nproc (int, opt):        Number of processors to use. default is 1
            loglevel (string, opt):  Logging level
            overwrite (bool, opt):   True to overwrite existing files in LRA,
                                     default is False
            definitive (bool, opt):  True to create the output file,
                                     False to just explore the reader
                                     operations, default is False
        """
        # General settings
        self.logger = log_configure(loglevel, 'lra_generator')
        self.loglevel = loglevel

        self.overwrite = overwrite
        if self.overwrite:
            self.logger.info('File will be overwritten if already existing.')

        self.definitive = definitive
        if not self.definitive:
            self.logger.warning('IMPORTANT: no file will be created, this is a dry run')

        self.nproc = int(nproc)
        self.tmpdir = tmpdir
        if self.nproc > 1:
            self.dask = True
            self.logger.info('Running dask.distributed with %s workers', self.nproc)
            if not self.tmpdir:
                raise KeyError('Please specify tmpdir for dask.distributed.')

            self.tmpdir = os.path.join(self.tmpdir, 'LRA_' +
                                        generate_random_string(10))
        else:
            self.dask = False

        # # Data settings
        # self._assign_key('model', model)
        # self._assign_key('exp', exp)
        # self._assign_key('source', source)

        if model:
            self.model = model
        else:
            raise KeyError('Please specify model.')

        if exp:
            self.exp = exp
        else:
            raise KeyError('Please specify experiment.')

        if source:
            self.source = source
        else:
            raise KeyError('Please specify source.')

        if not configdir:
            self.configdir = get_config_dir()
        else:
            self.configdir = configdir
        self.machine = get_machine(self.configdir)

        # Initialize variable(s)
        self.var = None
        if vars:
            self.var = vars
        else:
            self.var = var
        if not self.var:
            raise KeyError('Please specify variable string or list.')
        self.logger.info('Variable(s) to be processed: %s', self.var)

        self.resolution = resolution
        if not self.resolution:
            raise KeyError('Please specify resolution.')

        self.frequency = frequency
        if not self.frequency:
            self.logger.info('Frequency not specified, streaming mode')

        # option for time encoding, defined once for all
        self.time_encoding = {
            'units': 'days since 1970-01-01',
            'calendar': 'standard',
            'dtype': 'float64'
            }

        self.fix = fix
        self.logger.info('Fixing data: %s', self.fix)

        # Create LRA folder
        self.outdir = os.path.join(outdir, self.model, self.exp, self.resolution)

        if self.frequency:
            self.outdir = os.path.join(self.outdir, self.frequency)

        create_folder(self.outdir, loglevel=self.loglevel)

        # Initialize variables used by methods
        self.data = None
        self.reader = None
        self.cluster = None
        self.client = None

    # def _assign_key(self, name, key):

    #     """Assign the key and raise and error"""

    #     if key:
    #         setattr(self, name, key)
    #     else:
    #         raise KeyError('Please specify {name}.')

    def retrieve(self):
        """
        Retrieve data from the catalog
        """
        self.logger.info('Accessing catalog for %s-%s-%s...',
                         self.model, self.exp, self.source)
        if self.frequency:
            self.logger.info('I am going to produce LRA at %s resolution and %s frequency...',
                             self.resolution, self.frequency)
        else:
            self.logger.info('I am going to produce LRA at %s resolution...',
                             self.resolution)

        # Initialize the reader
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source,
                             regrid=self.resolution, freq=self.frequency,
                             configdir=self.configdir, loglevel=self.loglevel)

        self.logger.warning('Retrieving data...')
        self.data = self.reader.retrieve(fix=self.fix)
        self.logger.debug(self.data)

    def generate_lra(self):
        """
        Generate LRA data
        """
        self.logger.warning('Generating LRA data...')

        # Set up dask cluster
        self._set_dask()

        if isinstance(self.var, list):
            for var in self.var:
                self._write_var(var)
        else:  # Only one variable
            self._write_var(self.var)

        # Cleaning
        self.data.close()
        self._close_dask()
        #self._remove_tmpdir()

        self.logger.warning('Finished generating LRA data.')

    def create_catalog_entry(self):

        """
        Create an entry in the catalog for the LRA
        """

        entry_name = f'lra-{self.resolution}-{self.frequency}'
        self.logger.warning('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)

        # define the block to be uploaded into the catalog
        block_cat = {
            'driver': 'netcdf',
            'args': {
                'urlpath': os.path.join(self.outdir, f'*{self.exp}_{self.resolution}_{self.frequency}_????.nc'),
                'chunks': {},
                'xarray_kwargs': {
                    'decode_times': True
                }
            }
        }

        # find the catalog of my experiment
        catalogfile = os.path.join(self.configdir, self.machine,
                                   'catalog', self.model, self.exp+'.yaml')

        # load, add the block and close
        cat_file = load_yaml(catalogfile)
        cat_file['sources'][entry_name] = block_cat
        with open(catalogfile, 'w', encoding='utf-8') as file:
            yaml.dump(cat_file, file, sort_keys=False)


    def _set_dask(self):
        """
        Set up dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.logger.info('Setting up dask cluster with %s workers', self.nproc)
            dask.config.set({'temporary_directory': self.tmpdir})
            self.logger.info('Temporary directory: %s', self.tmpdir)
            self.cluster = LocalCluster(n_workers=self.nproc,
                                        threads_per_worker=1)
            self.client = Client(self.cluster)
        else:
            self.client = None
            dask.config.set(scheduler='synchronous')

    def _close_dask(self):
        """
        Close dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.client.shutdown()
            self.cluster.close()
            self.logger.info('Dask cluster closed')

    def _remove_tmpdir(self):
        """
        Remove temporary directory
        """
        if self.dask:  # self.nproc > 1
            self.logger.warning('Removing temporary directory %s', self.tmpdir)
            os.removedirs(self.tmpdir)

    def _file_is_complete(self, filename):

        """Basic check to see if file exists and that includes values which are not NaN
        Return a boolean that can be used as a flag for further operation
        True means that we have to re-do the computation"""

        if os.path.isfile(filename):
            self.logger.info('File %s is found...', filename)
            try:
                xfield = xr.open_dataset(filename)
                varname = list(xfield.data_vars)[0]
                if xfield[varname].isnull().all():
                #if xfield[varname].isnull().all(dim=['lon','lat']).all():
                    self.logger.error('File %s is full of NaN! Recomputing...', filename)
                    check=True
                else:
                    check=False
                    self.logger.info('File %s seems ok!', filename)
            # we have no clue which kind of exception might show up
            except BaseException:
                self.logger.info('Something wrong with file %s! Recomputing...', filename)
                check= True
        else:
            self.logger.info('File %s not found...', filename)
            check = True

        return check

    def _concat_var(self, var, year):
        """
        To reduce the amount of files concatenate together all the files 
        from the same year
        """

        infiles =  os.path.join(self.outdir,
                    f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}??.nc')
        xfield = xr.open_mfdataset(infiles)
        self.logger.warning('Creating a single file for %s, year %s...',  var, str(year))
        outfile = os.path.join(self.outdir,
                    f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}.nc')
        # clean older file
        if os.path.exists(outfile):
            os.remove(outfile)
        xfield.to_netcdf(outfile)

        # clean of monthly files
        for infile in glob.glob(infiles):
            self.logger.info('Cleaning %s...',  infile)
            os.remove(infile)

    def _write_var(self, var):
        """
        Write variable to file

        Args:
            var (str): variable name
        """
        t_beg = time()

        self.logger.warning('Processing variable %s...', var)
        temp_data = self.data[var]
        if self.frequency:
            temp_data = self.reader.timmean(temp_data)
        temp_data = self.reader.regrid(temp_data)

        # Splitting data into yearly files
        years = set(temp_data.time.dt.year.values)
        for year in years:

            self.logger.info('Processing year %s...', str(year))
            yearfile = os.path.join(self.outdir,
                                    f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}.nc')
            filecheck = self._file_is_complete(yearfile)
            if not filecheck and not self.overwrite:
                self.logger.warning('Yearly file %s already exists, skipping...', yearfile)
                continue

            year_data = temp_data.sel(time=temp_data.time.dt.year == year)
            # Splitting data into monthly files
            months = set(year_data.time.dt.month.values)
            for month in months:
                self.logger.info('Processing month %s...', str(month))
                outfile = os.path.join(self.outdir,
                                     f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}{str(month).zfill(2)}.nc')
                # checking if file is there and is complete
                filecheck = self._file_is_complete(outfile)
                if not filecheck and not self.overwrite:
                    self.logger.warning('Monthly file %s already exists, skipping...', outfile)
                    continue
                month_data = year_data.sel(time=year_data.time.dt.month == month)
                self.logger.debug(month_data)

                # real writing
                if self.definitive:
                    self._write_var_month(month_data, outfile)

                    # check everything is correct
                    self._file_is_complete(outfile)
                    # we can later add a retry
                    if not filecheck:
                        self.logger.error('Something has gone wrong in %s!', outfile)
                del month_data
            del year_data
            if self.definitive:
                self._concat_var(var, year)
        del temp_data

        t_end = time()
        self.logger.info('Process took {:.4f} seconds'.format(t_end-t_beg))


    def _write_var_month(self, month_data, outfile):
        """Write a single chunk of data - Xarray Dataset - to a specific file 
        using dask if required and monitoring the progress"""

        # File to be written
        if os.path.exists(outfile):
            os.remove(outfile)
            self.logger.warning('Overwriting file %s...', outfile)

        self.logger.warning('Writing file %s...', outfile)

        # Write data to file, lazy evaluation
        write_job =\
            month_data.to_netcdf(outfile,
                                    encoding={'time':
                                            self.time_encoding},
                                    compute=False)

        if self.dask:
            w_job = write_job.persist()
            progress(w_job)
            del w_job
        else:
            with ProgressBar():
                write_job.compute()

        del write_job
        self.logger.info('Writing file %s successfull!', outfile)

"""
LRA class for glob
"""

import os
import types
from time import time
import glob
import shutil
import dask
import xarray as xr
import numpy as np
import pandas as pd
from dask.distributed import Client, LocalCluster, progress, performance_report
from dask.diagnostics import ProgressBar
from dask.distributed.diagnostics import MemorySampler
from aqua.logger import log_configure, log_history
from aqua.reader import Reader
from aqua.util import create_folder, generate_random_string
from aqua.util import dump_yaml, load_yaml
from aqua.util import ConfigPath, file_is_complete
from aqua.lra_generator.lra_util import move_tmp_files

#from aqua.lra_generator.lra_util import check_correct_ifs_fluxes


class LRAgenerator():
    """
    Class to generate LRA data at required frequency/resolution
    """

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(self,
                 model=None, exp=None, source=None, zoom=None,
                 var=None, configdir=None,
                 resolution=None, frequency=None, fix=True,
                 outdir=None, tmpdir=None, nproc=1,
                 loglevel=None, overwrite=False, definitive=False,
                 performance_reporting=False,
                 exclude_incomplete=False):
        """
        Initialize the LRA_Generator class

        Args:
            model (string):          The model name from the catalog
            exp (string):            The experiment name from the catalog
            source (string):         The sourceid name from the catalog
            var (str, list):         Variable(s) to be processed and archived
                                     in LRA.
            zoom (int):              Healpix level of zoom
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
            exclude_incomplete (bool,opt)   : True to remove incomplete chunk
                                            when averaging, default is false.  
        """
        # General settings
        self.logger = log_configure(loglevel, 'lra_generator')
        self.loglevel = loglevel

        self.overwrite = overwrite
        if self.overwrite:
            self.logger.warning('File will be overwritten if already existing.')

        self.exclude_incomplete = exclude_incomplete
        if self.exclude_incomplete:
            self.logger.info('Exclude incomplete for time averaging activated!')

        self.definitive = definitive
        if not self.definitive:
            self.logger.warning('IMPORTANT: no file will be created, this is a dry run')

        self.nproc = int(nproc)
        self.tmpdir = tmpdir
        if self.nproc > 1:
            self.logger.info('Running dask.distributed with %s workers', self.nproc)
            if not self.tmpdir:
                raise KeyError('Please specify tmpdir for dask.distributed.')

            self.tmpdir = os.path.join(self.tmpdir, 'LRA_' +
                                       generate_random_string(10))

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

        self.zoom = zoom

        Configurer = ConfigPath(configdir=configdir)
        self.configdir = Configurer.configdir
        self.machine = Configurer.machine

        # Initialize variable(s)
        self.var = var

        if not self.var:
            raise KeyError('Please specify variable string or list.')
        self.logger.info('Variable(s) to be processed: %s', self.var)

        self.resolution = resolution
        if not self.resolution:
            raise KeyError('Please specify resolution.')

        self.frequency = frequency
        if not self.frequency:
            self.logger.info('Frequency not specified, no time averagin will be performed.')

        # option for time encoding, defined once for all
        self.time_encoding = {
            'units': 'days since 1850-01-01 00:00:00',
            'calendar': 'standard',
            'dtype': 'float64',
            'zlib' : True,
            'complevel': 1,
            '_FillValue': np.nan
        }

        # add the performance report
        self.performance_reporting = performance_reporting

        self.fix = fix
        self.logger.info('Fixing data: %s', self.fix)

        # for data reading from FDB
        self.last_record = None
        self.check = False

        # Create LRA folder
        self.outdir = os.path.join(outdir, self.model, self.exp, self.resolution)

        if self.frequency:
            self.outdir = os.path.join(self.outdir, self.frequency)

        create_folder(self.outdir, loglevel=self.loglevel)
        create_folder(self.tmpdir, loglevel=self.loglevel)

        # Initialize variables used by methods
        self.data = None
        self.cluster = None
        self.client = None
        self.reader = None

    def retrieve(self):
        """
        Retrieve data from the catalog
        """

        # Initialize the reader
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source, zoom=self.zoom,
                             regrid=self.resolution,
                             loglevel=self.loglevel,
                             fix=self.fix)

        self.logger.info('Accessing catalog for %s-%s-%s...',
                         self.model, self.exp, self.source)
        if self.frequency:
            self.logger.info('I am going to produce LRA at %s resolution and %s frequency...',
                             self.resolution, self.frequency)
        else:
            self.logger.info('I am going to produce LRA at %s resolution...',
                             self.resolution)

        self.logger.info('Retrieving data...')
        self.data = self.reader.retrieve(var=self.var)
        
        self.logger.debug(self.data)

    def generate_lra(self):
        """
        Generate LRA data
        """
        self.logger.info('Generating LRA data...')

        # Set up dask cluster
        self._set_dask()

        if isinstance(self.var, list):
            for var in self.var:
                self._write_var(var)

        else:  # Only one variable
            self._write_var(self.var)
                
        self.logger.info('Move tmp files to output directory')
        move_tmp_files(self.tmpdir, self.outdir)
            
        # Cleaning
        self.data.close()
        self._close_dask()
        self._remove_tmpdir()

        self.logger.info('Finished generating LRA data.')

    def create_catalog_entry(self):
        """
        Create an entry in the catalog for the LRA
        """

        entry_name = f'lra-{self.resolution}-{self.frequency}'
        urlpath = os.path.join(self.outdir, f'*{self.exp}_{self.resolution}_{self.frequency}_*.nc')
        self.logger.info('Creating catalog entry %s %s %s', self.model, self.exp, entry_name)

        # define the block to be uploaded into the catalog
        block_cat = {
            'driver': 'netcdf',
            'description': f'LRA data {self.frequency} at {self.resolution}',
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

        # find the catalog of my experiment
        catalogfile = os.path.join(self.configdir, 'machines', self.machine,
                                   'catalog', self.model, self.exp + '.yaml')

        # load, add the block and close
        cat_file = load_yaml(catalogfile)
        if entry_name in cat_file['sources']:
            self.logger.info('Catalog entry for %s %s %s exists, updating the urlpath only...',
                             self.model, self.exp, entry_name)
            cat_file['sources'][entry_name]['args']['urlpath'] = urlpath
        else:
            cat_file['sources'][entry_name] = block_cat
        dump_yaml(outfile=catalogfile, cfg=cat_file)

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
        self.logger.info('Removing temporary directory %s', self.tmpdir)
        shutil.rmtree(self.tmpdir)

    def _concat_var_year(self, var, year):
        """
        To reduce the amount of files concatenate together all the files
        from the same year
        """

        #infiles = os.path.join(self.outdir,
        #                       f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}??.nc')
        infiles = self.get_filename(var, year, month = '??')
        if len(glob.glob(infiles)) == 12:
            xfield = xr.open_mfdataset(infiles)
            self.logger.info('Creating a single file for %s, year %s...', var, str(year))
            outfile = self.get_filename(var, year)
            #outfile = os.path.join(self.tmpdir,
            #                       f'{var}_{self.exp}_{self.resolution}_{self.frequency}_{year}.nc')
            # clean older file
            if os.path.exists(outfile):
                os.remove(outfile)
            xfield.to_netcdf(outfile)

            # clean of monthly files
            for infile in glob.glob(infiles):
                self.logger.info('Cleaning %s...', infile)
                os.remove(infile)


    def get_filename(self, var, year=None, month=None, tmp=False):
        """Create output filenames"""

        filestring = f'{var}_{self.exp}_{self.resolution}_{self.frequency}_*.nc'
        if tmp:
            filename = os.path.join(self.tmpdir, filestring)
        else:
            filename = os.path.join(self.outdir, filestring)

        if (year is not None) and (month is None):
            filename = filename.replace("*", str(year))
        if (year is not None) and (month is not None):
            filename = filename.replace("*", str(year) + str(month).zfill(2))

        return filename

    def check_integrity(self, varname):
        """To check if the LRA entry is fine before running"""

        yearfiles = self.get_filename(varname)
        yearfiles = glob.glob(yearfiles)
        checks = [file_is_complete(yearfile, loglevel=self.loglevel) for yearfile in yearfiles]
        all_checks_true = all(checks) and len(checks) > 0
        if all_checks_true and not self.overwrite:
            self.logger.info('All the data produced seems complete for var %s...', varname)
            last_record = xr.open_mfdataset(self.get_filename(varname)).time[-1].values
            self.last_record = pd.to_datetime(last_record).strftime('%Y%m%d')
            self.check = True
            self.logger.info('Last record archived is %s...', self.last_record)
        else:
            self.check = False
            self.logger.warning('Still need to run for var %s...', varname)

    def _write_var(self, var):
        """Call write var for generator or catalog access"""
        t_beg = time()

        if isinstance(self.data, types.GeneratorType):
            raise ValueError('Generator no longer supported by AQUA LRA.')
        else:
            #if not self.check:
            self._write_var_catalog(var)

        t_end = time()
        self.logger.info('Process took {:.4f} seconds'.format(t_end - t_beg))

    def _remove_regridded(self, data):

        # remove regridded attribute to avoid issues with Reader
        # https://github.com/oloapinivad/AQUA/issues/147
        if 'regridded' in data.attrs:
            self.logger.debug('Removing regridding attribute...')
            del data.attrs['regridded']
        return data

    # def _write_var_generator(self, var):
    #     """
    #     Write a variable to file using the GSV generator
    #     """

    #     # supplementary retrieve tu use the generator
    #     self.data = self.reader.retrieve(var=var, startdate=self.last_record)
    #     self.logger.info('Looping on generator data...')
    #     t_beg = time()
    #     for data in self.data:

    #         temp_data = data[var]
    #         self.logger.info('Generator returned data from %s to %s', temp_data.time[0].values, temp_data.time[-1].values)

    #         if self.frequency:
    #             temp_data = self.reader.timmean(temp_data)
    #         temp_data = self.reader.regrid(temp_data)

    #         temp_data = self._remove_regridded(temp_data)

    #         year = temp_data.time.dt.year.values[0]
    #         month = temp_data.time.dt.month.values[0]

    #         yearfile = self.get_filename(var, year = year)
    #         filecheck = file_is_complete(yearfile, loglevel=self.loglevel)
    #         if filecheck:
    #             if not self.overwrite:
    #                 self.logger.info('Yearly file %s already exists, skipping...', yearfile)
    #                 continue
    #             else:
    #                 self.logger.warning('Yearly file %s already exists, overwriting as requested...', yearfile)

    #         self.logger.info('Processing year %s month %s...', str(year), str(month))
    #         outfile = self.get_filename(var, year = year, month = month)

    #         # checking if file is there and is complete
    #         filecheck = file_is_complete(outfile, loglevel=self.loglevel)
    #         if filecheck:
    #             if not self.overwrite:
    #                 self.logger.info('Monthly file %s already exists, skipping...', outfile)
    #                 continue
    #             else:
    #                 self.logger.warning('Monthly file %s already exists, overwriting as requested...', outfile)
            
    #         # real writing
    #         if self.definitive:
    #             self.write_chunk(temp_data, outfile)

    #             # check everything is correct
    #             filecheck = file_is_complete(outfile, loglevel=self.loglevel)
    #             # we can later add a retry
    #             if not filecheck:
    #                 self.logger.error('Something has gone wrong in %s!', outfile)

    #         if self.definitive and month == 12:
    #             self._concat_var_year(var, year)

    #         self.logger.info('Processing this chunk took {:.4f} seconds'.format(time() - t_beg))
    #         t_beg = time()

    def _write_var_catalog(self, var):
        """
        Write variable to file

        Args:
            var (str): variable name
        """

        self.logger.info('Processing variable %s...', var)
        temp_data = self.data[var]

        if self.frequency:
            temp_data = self.reader.timmean(temp_data, freq=self.frequency,
                                            exclude_incomplete=self.exclude_incomplete)

        # regrid
        temp_data = self.reader.regrid(temp_data)
        temp_data = self._remove_regridded(temp_data)

        # Splitting data into yearly files
        years = sorted(set(temp_data.time.dt.year.values))
        if self.performance_reporting:
            years = [years[0]]
        for year in years:

            self.logger.info('Processing year %s...', str(year))
            yearfile = self.get_filename(var, year = year)

            # checking if file is there and is complete
            filecheck = file_is_complete(yearfile, loglevel=self.loglevel)
            if filecheck:
                if not self.overwrite:
                    self.logger.info('Yearly file %s already exists, skipping...', yearfile)
                    continue
                else:
                    self.logger.warning('Yearly file %s already exists, overwriting as requested...', yearfile)
            year_data = temp_data.sel(time=temp_data.time.dt.year == year)

            # Splitting data into monthly files
            months = sorted(set(year_data.time.dt.month.values))
            if self.performance_reporting:
                months = [months[0]]
            for month in months:
                self.logger.info('Processing month %s...', str(month))
                outfile = self.get_filename(var, year = year, month = month)

                # checking if file is there and is complete
                filecheck = file_is_complete(outfile, loglevel=self.loglevel)
                if filecheck:
                    if not self.overwrite:
                        self.logger.info('Monthly file %s already exists, skipping...', outfile)
                        continue
                    else:
                        self.logger.warning('Monthly file %s already exists, overwriting as requested...', outfile)

                month_data = year_data.sel(time=year_data.time.dt.month == month)

                #self.logger.debug(month_data.mean().values)
                #self.logger.debug(month_data)

                # real writing
                if self.definitive:
                    tmpfile = self.get_filename(var, year = year, month = month, tmp = True)
                    schunk = time()
                    self.write_chunk(month_data, tmpfile)
                    tchunk = time() - schunk
                    self.logger.info('Chunk execution time: %.2f', tchunk)

                    # check everything is correct
                    filecheck = file_is_complete(tmpfile, loglevel=self.loglevel)
                    # we can later add a retry
                    if not filecheck:
                        self.logger.error('Something has gone wrong in %s!', tmpfile)
                    move_tmp_files(self.tmpdir, self.outdir)
                del month_data
            del year_data
            if self.definitive:
                self._concat_var_year(var, year)
        del temp_data

    def write_chunk(self, data, outfile):
        """Write a single chunk of data - Xarray Dataset - to a specific file
        using dask if required and monitoring the progress"""
        
        # update data attributes for history
        if self.frequency:
            log_history(data, f'regridded from {self.reader.src_grid_name} to {self.resolution} and from frequency {self.reader.orig_freq} to {self.frequency} through LRA generator')                
        else:
            log_history(data, f'regridded from {self.reader.src_grid_name} to {self.resolution} through LRA generator')

        # File to be written
        if os.path.exists(outfile):
            os.remove(outfile)
            self.logger.warning('Overwriting file %s...', outfile)

        self.logger.info('Writing file %s...', outfile)

        # Write data to file, lazy evaluation
        write_job = data.to_netcdf(outfile,
                                   encoding={'time': self.time_encoding},
                                   compute=False)

        if self.dask:
            # optional full stack dashboard to html
            if self.performance_reporting:
                filename = f"dask-{self.model}-{self.exp}-{self.source}-{self.nproc}.html"
                with performance_report(filename=filename):
                    w_job = write_job.persist()
                    progress(w_job)
                    del w_job
            else:
                # memory monitoring is always operating
                ms = MemorySampler()
                with ms.sample('chunk'):
                    w_job = write_job.persist()
                    progress(w_job)
                    del w_job
                array_data = np.array(vars(ms)['samples']['chunk'])
                avg_mem = np.mean(array_data[:, 1])/1e9
                max_mem = np.max(array_data[:, 1])/1e9
                self.logger.info('Avg memory used: %.2f GiB, Peak memory used: %.2f GiB', avg_mem, max_mem)
                
        else:
            with ProgressBar():
                write_job.compute()

        del write_job
        self.logger.info('Writing file %s successfull!', outfile)

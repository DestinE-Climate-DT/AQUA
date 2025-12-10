"""
DROP (Data Reduction OPerator) class

This class provides comprehensive data processing capabilities for climate datasets,
including regridding, temporal averaging, regional extraction, and archiving.
It handles multiple file formats and uses Dask for parallel processing of large datasets.

Main features:
- Regridding to arbitrary resolutions
- Temporal resampling with various statistics (mean, std, max, min)
- Regional data extraction
- Automatic catalog entry generation
- Parallel processing with Dask
- Memory-efficient chunked processing
"""
import os
from time import time
import subprocess
import glob
import shutil
import dask
import xarray as xr
import numpy as np
import pandas as pd

from dask.distributed import Client, LocalCluster, progress, performance_report
from dask.diagnostics import ProgressBar
from dask.distributed.diagnostics import MemorySampler

from aqua.core.lock import SafeFileLock
from aqua.core.logger import log_configure, log_history
from aqua.core.reader import Reader
from aqua.core.util.io_util import create_folder, file_is_complete
from aqua.core.util import dump_yaml, load_yaml
from aqua.core.configurer import ConfigPath
from aqua.core.util import create_zarr_reference, replace_intake_vars
from aqua.core.util.string import generate_random_string
from .drop_util import move_tmp_files, list_drop_files_complete
from .catalog_entry_builder import CatalogEntryBuilder


TIME_ENCODING = {
    'units': 'days since 1850-01-01 00:00:00',
    'calendar': 'standard',
    'dtype': 'float64'}

VAR_ENCODING = {
    'dtype': 'float64',
    'zlib': True,
    'complevel': 1,
    '_FillValue': np.nan
}


class Drop():
    """
    Class to generate DROP outputs at required frequency/resolution
    """

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(self,
                 catalog=None, model=None, exp=None, source=None,
                 var=None, configdir=None,
                 resolution=None, frequency=None, fix=True,
                 startdate=None, enddate=None,
                 outdir=None, tmpdir=None, nproc=1,
                 loglevel=None,
                 region=None, drop=False,
                 overwrite=False, definitive=False,
                 performance_reporting=False,
                 rebuild=False,
                 exclude_incomplete=False,
                 stat="mean",
                 compact="xarray",
                 cdo_options=["-f", "nc4", "-z", "zip_1"],
                 **kwargs):
        """
        Initialize the DROP class

        Args:
            catalog (string):        The catalog you want to read. If None, guessed by the reader.
            model (string):          The model name from the catalog
            exp (string):            The experiment name from the catalog
            source (string):         The sourceid name from the catalog
            var (str, list):         Variable(s) to be processed and archived.
            resolution (string):     The target resolution for the DROP output. If None,
                                     no regridding is performed.
            frequency (string,opt):  The target frequency for averaging the
                                     DROP output, if no frequency is specified,
                                     no time average is performed
            fix (bool, opt):         True to fix the data, default is True
            startdate (string,opt): Start date for the data to be processed,
                                     format YYYYMMDD, default is None
            enddate (string,opt):   End date for the data to be processed,
                                     format YYYYMMDD, default is None
            outdir (string):         Where the DROP output is stored.
            tmpdir (string):         Where to store temporary files,
                                     default is None.
                                     Necessary for dask.distributed
            configdir (string):      Configuration directory where the catalog
                                     are found
            nproc (int, opt):        Number of processors to use. default is 1
            loglevel (string, opt):  Logging level
            region (dict, opt):      Region to be processed, default is None,
                                     meaning 'global'.
                                     Requires 'name' (str), 'lon' (list) and 'lat' (list)
            drop (bool, opt):        Drop the missing values in the region selection.
            overwrite (bool, opt):   True to overwrite existing files, default is False
            definitive (bool, opt):  True to create the output file,
                                     False to just explore the reader
                                     operations, default is False
            performance_reporting (bool, opt): True to save an html report of the
                                               dask usage, default is False. This will run a single month
                                               to collect the performance data.
            exclude_incomplete (bool,opt)   : True to remove incomplete chunk
                                            when averaging, default is false.
            rebuild (bool, opt):     Rebuild the weights when calling the reader
            stat (string, opt):      Statistic to compute. Can be 'mean', 'std', 'max', 'min'.
            compact (string, opt):   Compact the data into yearly files using xarray or cdo.
                                     If set to None, no compacting is performed. Default is "xarray"
            cdo_options (list, opt): List of options to be passed to cdo, default is ["-f", "nc4", "-z", "zip_1"]
            **kwargs:                kwargs to be sent to the Reader, as 'zoom' or 'realization'
        """

        # Check mandatory parameters
        self.catalog = self._require_param(catalog, "catalog")
        self.model = self._require_param(model, "model")
        self.exp = self._require_param(exp, "experiment")
        self.source = self._require_param(source, "source")
        self.var = self._require_param(var, "variable string or list.")

        # General settings
        self.logger = log_configure(loglevel, 'DROP')
        self.loglevel = loglevel

        # save parameters
        self.resolution = resolution
        self.frequency = frequency
        self.overwrite = overwrite
        self.exclude_incomplete = exclude_incomplete
        self.definitive = definitive
        self.nproc = int(nproc)
        self.rebuild = rebuild
        self.kwargs = kwargs
        self.fix = fix

        # configure start date and end date
        self.startdate = startdate
        self.enddate = enddate
        self._validate_date(startdate, enddate)

        # configure statistics
        self.stat = stat
        if self.stat not in ['mean', 'std', 'max', 'min']:
            raise KeyError('Please specify a valid statistic: mean, std, max or min.')

        # configure regional selection
        self._configure_region(region, drop)

        # print some info about the settings
        self._issue_info_warning()

        # define the tmpdir
        if tmpdir is None:
            self.logger.warning('No tmpdir specifield, will use outdir')
            self.tmpdir = os.path.join(outdir, 'tmp')
        else:
            self.tmpdir = tmpdir
        self.tmpdir = os.path.join(self.tmpdir, f'DROP_{generate_random_string(10)}')

        # set up compacting method for concatenation
        self.compact = compact
        if self.compact not in ['xarray', 'cdo', None]:
            raise KeyError('Please specify a valid compact method: xarray, cdo or None.')

        self.cdo_options = cdo_options
        if not isinstance(self.cdo_options, list):
            raise TypeError('cdo_options must be a list.')

        # configure the configdir
        configpath = ConfigPath(configdir=configdir)
        self.configdir = configpath.configdir

        # get default grids
        _, grids_path = configpath.get_reader_filenames()
        self.default_grids = load_yaml(os.path.join(grids_path, 'default.yaml'))

        # option for encoding, defined once for all
        self.time_encoding = TIME_ENCODING
        self.var_encoding = VAR_ENCODING

        # add the performance report
        self.performance_reporting = performance_reporting

        # Create output folders
        if outdir is None:
            raise KeyError('Please specify outdir.')

        self.catbuilder = CatalogEntryBuilder(
            catalog=self.catalog, model=self.model,
            exp=self.exp, resolution=self.resolution, frequency=self.frequency,
            region=self.region_name, stat=self.stat, loglevel=self.loglevel, **self.kwargs
        )
        # Create output path builder from the catalog entry builder
        self.outbuilder = self.catbuilder.opt

        self.basedir = outdir
        self.outdir = os.path.join(self.basedir, self.outbuilder.build_directory())

        create_folder(self.outdir, loglevel=self.loglevel)
        create_folder(self.tmpdir, loglevel=self.loglevel)

        # Initialize variables used by methods
        self.data = None
        self.cluster = None
        self.client = None
        self.reader = None

        # for data reading from FDB
        self.last_record = None
        self.check = False

    @staticmethod
    def _require_param(param, name, msg=None):
        if param is not None:
            return param
        raise KeyError(msg or f"Please specify {name}.")

    @staticmethod
    def _validate_date(startdate, enddate):
        """Validate date format for startdate and enddate"""
        
        if startdate is not None:
            try:
                pd.to_datetime(startdate)
            except (ValueError, TypeError):
                raise ValueError('startdate must be a valid date string (YYYY-MM-DD or YYYYMMDD)')
        
        if enddate is not None:
            try:
                pd.to_datetime(enddate)
            except (ValueError, TypeError):
                raise ValueError('enddate must be a valid date string (YYYY-MM-DD or YYYYMMDD)')

    def _issue_info_warning(self):
        """
        Print information about the DROP settings
        """

        if self.startdate is not None or self.enddate is not None:
            self.logger.info('startdate is %s, enddate is %s', self.startdate, self.enddate)
            self.logger.info('startdate or enddate are set, please be sure to process one experiment at the time.')

        if not self.frequency:
            self.logger.info('Frequency not specified, no time averaging will be performed.')
        else:
            self.logger.info('Frequency: %s', self.frequency)

        if self.overwrite:
            self.logger.warning('File will be overwritten if already existing.')

        if self.exclude_incomplete:
            self.logger.info('Exclude incomplete for time averaging activated!')

        if not self.definitive:
            self.logger.warning('IMPORTANT: no file will be created, this is a dry run')

        if self.dask:
            self.logger.info('Running dask.distributed with %s workers', self.nproc)

        if self.rebuild:
            self.logger.info('rebuild=True! DROP will rebuild weights and areas!')

        self.logger.info('Variable(s) to be processed: %s', self.var)
        self.logger.info('Fixing data: %s', self.fix)
        self.logger.info('Resolution: %s', self.resolution)
        self.logger.info('Statistic to be computed: %s', self.stat)
        self.logger.info('Domain selection: %s', self.region_name)

    def _configure_region(self, region, drop):
        """ Configure the region for regional selection, and the drop option"""

        if region is not None:
            self.region = region
            if self.region['name'] is None:
                raise KeyError('Please specify name in region.')
            if self.region['lon'] is None and self.region['lat'] is None:
                raise KeyError(f"Please specify at least one between lat and lon for {region['name']}.")
            self.region_name = self.region['name']
            self.logger.info('Regional selection active! region: %s, lon: %s and lat: %s...',
                             self.region['name'], self.region['lon'], self.region['lat'])
        else:
            self.region = None
            self.region_name = None
        self.drop = drop

    def retrieve(self):
        """
        Retrieve data from the catalog
        """

        # Initialize the reader
        self.reader = Reader(model=self.model, exp=self.exp,
                             source=self.source,
                             regrid=self.resolution,
                             catalog=self.catalog,
                             loglevel=self.loglevel,
                             rebuild=self.rebuild,
                             startdate=self.startdate,
                             enddate=self.enddate,
                             fix=self.fix, **self.kwargs)

        self.logger.info('Accessing catalog for %s-%s-%s...',
                         self.model, self.exp, self.source)

        if self.catalog is None:
            self.logger.info('Assuming catalog from the reader so that is %s', self.reader.catalog)
            self.catalog = self.reader.catalog

        self.logger.info('Retrieving data...')
        self.data = self.reader.retrieve(var=self.var)

        self.logger.debug(self.data)

    def drop_generator(self):
        """
        Generate DROP output
        """
        self.logger.info('Generating DROP output...')

        # Set up dask cluster
        self._set_dask()

        if isinstance(self.var, list):
            for var in self.var:
                self._write_var(var)

        else:  # Only one variable
            self._write_var(self.var)

        self.logger.info('Move tmp files from %s to output directory %s', self.tmpdir, self.outdir)
        # Move temporary files to output directory
        move_tmp_files(self.tmpdir, self.outdir)

        # Cleaning
        self.data.close()
        self._close_dask()
        self._remove_tmpdir()

        self.logger.info('Finished generating DROP output.')

    def _define_source_grid_name(self):
        """"
        Define the source grid name based on the resolution
        """
        if self.resolution in self.default_grids:
            return 'lon-lat'
        if self.resolution == 'native':
            try:
                return self.reader.source_grid_name
            except AttributeError:
                self.logger.warning('No source grid name defined in the reader, using resolution as source grid name')
                return False
        return self.resolution

    def create_catalog_entry(self):
        """
        Create an entry in the catalog for DROP
        """
        # find the catalog of my experiment and load it
        catalogfile = os.path.join(self.configdir, 'catalogs', self.catalog,
                                   'catalog', self.model, self.exp + '.yaml')
        
        with SafeFileLock(catalogfile + '.lock', loglevel=self.loglevel):
            cat_file = load_yaml(catalogfile)

            # define the entry name
            entry_name = self.catbuilder.create_entry_name()
            sgn = self._define_source_grid_name()

            if entry_name in cat_file['sources']:
                catblock = cat_file['sources'][entry_name]
            else:
                catblock = None

            block = self.catbuilder.create_entry_details(
                basedir=self.basedir, catblock=catblock, 
                source_grid_name=sgn
            )

            cat_file['sources'][entry_name] = block

            # dump the update file
            dump_yaml(outfile=catalogfile, cfg=cat_file)

    def create_zarr_entry(self, verify=True):
        """
        Create a Zarr entry in the catalog for DROP

        Args:
            verify: open the DROP source and verify it can be read by the reader
        """
        full_dict, partial_dict = list_drop_files_complete(self.outdir)

        # extra zarr only directory
        zarrdir = os.path.join(self.outdir, 'zarr')
        create_folder(zarrdir)

        # this dictionary based structure is an overkill but guarantee flexibility
        urlpath = []
        for key, value in full_dict.items():
            jsonfile = os.path.join(zarrdir, f'drop-yearly-{key}.json')
            self.logger.debug('Creating zarr files for full files %s', key)
            if value:
                jsonfile = create_zarr_reference(value, jsonfile, loglevel=self.loglevel)
                if jsonfile is not None:
                    urlpath = urlpath + [f'reference::{jsonfile}']

        for key, value in partial_dict.items():
            jsonfile = os.path.join(zarrdir, f'drop-monthly-{key}.json')
            self.logger.debug('Creating zarr files for partial files %s', key)
            if value:
                jsonfile = create_zarr_reference(value, jsonfile, loglevel=self.loglevel)
                if jsonfile is not None:
                    urlpath = urlpath + [f'reference::{jsonfile}']

        if not urlpath:
            raise FileNotFoundError('No files found to create zarr reference')

        # apply intake replacement: works on string need to loop on the list
        for index, value in enumerate(urlpath):
            urlpath[index] = replace_intake_vars(catalog=self.catalog, path=value)

        # find the catalog of my experiment and load it
        catalogfile = os.path.join(self.configdir, 'catalogs', self.catalog,
                                   'catalog', self.model, self.exp + '.yaml')
        
        with SafeFileLock(catalogfile + '.lock', loglevel=self.loglevel):
            cat_file = load_yaml(catalogfile)

            # define the entry name - zarr entries never have lra- prefix
            base_name = f'{self.catbuilder.resolution}-{self.catbuilder.frequency}'
            entry_name = base_name + '-zarr'
            self.logger.info('Creating zarr files for %s %s %s', self.model, self.exp, entry_name)
            sgn = self._define_source_grid_name()

            if entry_name in cat_file['sources']:
                catblock = cat_file['sources'][entry_name]
            else:
                catblock = None

            block = self.catbuilder.create_entry_details(
                basedir=self.basedir, catblock=catblock, source_grid_name=sgn, driver='zarr'
            )
            block['args']['urlpath'] = urlpath
            cat_file['sources'][entry_name] = block

            dump_yaml(outfile=catalogfile, cfg=cat_file)

        # verify the zarr entry makes sense
        if verify:
            self.logger.info('Verifying that zarr entry can be loaded...')
            try:
                reader = Reader(model=self.model, exp=self.exp, source=entry_name)
                _ = reader.retrieve()
                self.logger.info('Zarr entry successfully created!!!')
            except (KeyError, ValueError) as e:
                self.logger.error('Cannot load zarr DROP with error --> %s', e)
                self.logger.error('Zarr source is not accessible by the Reader likely due to irregular amount of NetCDF file')
                self.logger.error('To avoid issues in the catalog, the entry will be removed')
                self.logger.error('In case you want to keep it, please run with verify=False')
                with SafeFileLock(catalogfile + '.lock', loglevel=self.loglevel):
                    cat_file = load_yaml(catalogfile)
                    del cat_file['sources'][entry_name]
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

        infiles_pattern = self.get_filename(var, year, month='??')
        monthly_files = sorted(glob.glob(infiles_pattern))

        if len(monthly_files) == 12:
            self.logger.info('Creating a single file for %s, year %s...', var, str(year))
            outfile = self.get_filename(var, year)
            tmp_outfile = self.get_filename(var, year, tmp=True)
            
            # Move monthly files to tmp for safety
            for monthly_file in monthly_files:
                shutil.move(monthly_file, self.tmpdir)
            
            # Clean any existing output files
            for f in [tmp_outfile, outfile]:
                if os.path.exists(f):
                    os.remove(f)

            # Get the moved files in tmpdir - they keep the same basename
            tmp_monthly_files = [os.path.join(self.tmpdir, os.path.basename(f)) for f in monthly_files]

            # Concatenation with CDO or Xarray
            if self.compact == 'cdo':
                command = [
                    'cdo',
                    *self.cdo_options,
                    'cat',
                    *tmp_monthly_files,
                    tmp_outfile
                ]
                self.logger.debug("Using CDO command: %s", command)
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            else:
                self.logger.debug("Using xarray to concatenate files")
                xfield = xr.open_mfdataset(tmp_monthly_files, combine='by_coords', parallel=True)
                name = list(xfield.data_vars)[0]
                xfield.to_netcdf(tmp_outfile,
                                 encoding={'time': self.time_encoding, name: self.var_encoding})

            # Move back the yearly file and cleanup
            shutil.move(tmp_outfile, outfile)
            for tmp_file in tmp_monthly_files:
                self.logger.info('Cleaning %s...', tmp_file)
                os.remove(tmp_file)

    def get_filename(self, var, year=None, month=None, tmp=False):
        """Create output filenames"""

        filename = self.outbuilder.build_filename(var=var, year=year, month=month)

        if tmp:
            filename = os.path.join(self.tmpdir, filename)
        else:
            filename = os.path.join(self.outdir, filename)

        return filename

    def check_integrity(self, varname):
        """To check if the DROP entry is fine before running"""

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

        self._write_var_catalog(var)

        t_end = time()
        self.logger.info('Process took %.4f seconds', t_end - t_beg)

    def _remove_regridded(self, data):

        # remove regridded attribute to avoid issues with Reader
        # https://github.com/oloapinivad/AQUA/issues/147
        if "AQUA_regridded" in data.attrs:
            self.logger.debug('Removing regridding attribute...')
            del data.attrs["AQUA_regridded"]
        return data

    def _write_var_catalog(self, var):
        """
        Write variable to file

        Args:
            var (str): variable name
        """

        self.logger.info('Processing variable %s...', var)
        temp_data = self.data[var]

        if self.frequency:
            temp_data = self.reader.timstat(temp_data, self.stat, freq=self.frequency,
                                            exclude_incomplete=self.exclude_incomplete)

        # temp_data could be empty after time statistics if everything was excluded
        if 'time' in temp_data.coords and len(temp_data.time) == 0:
            self.logger.warning('No data available for variable %s after time statistics, skipping...', var)
            return
        
        # regrid
        if self.resolution:
            temp_data = self.reader.regrid(temp_data)
            temp_data = self._remove_regridded(temp_data)

        if self.region:
            temp_data = self.reader.select_area(temp_data, lon=self.region['lon'], lat=self.region['lat'], drop=self.drop)

        # Splitting data into yearly files
        years = sorted(set(temp_data.time.dt.year.values))
        if self.performance_reporting:
            years = [years[0]]
        for year in years:

            self.logger.info('Processing year %s...', str(year))
            yearfile = self.get_filename(var, year=year)

            # checking if file is there and is complete
            filecheck = file_is_complete(yearfile, loglevel=self.loglevel)
            if filecheck:
                if not self.overwrite:
                    self.logger.info('Yearly file %s already exists, skipping...', yearfile)
                    continue
                self.logger.warning('Yearly file %s already exists, overwriting as requested...', yearfile)
            year_data = temp_data.sel(time=temp_data.time.dt.year == year)

            # Splitting data into monthly files
            months = sorted(set(year_data.time.dt.month.values))
            if self.performance_reporting:
                months = [months[0]]
            for month in months:
                self.logger.info('Processing month %s...', str(month))
                outfile = self.get_filename(var, year=year, month=month)

                # checking if file is there and is complete
                filecheck = file_is_complete(outfile, loglevel=self.loglevel)
                if filecheck:
                    if not self.overwrite:
                        self.logger.info('Monthly file %s already exists, skipping...', outfile)
                        continue
                    self.logger.warning('Monthly file %s already exists, overwriting as requested...', outfile)

                month_data = year_data.sel(time=year_data.time.dt.month == month)

                # real writing
                if self.definitive:
                    tmpfile = self.get_filename(var, year=year, month=month, tmp=True)
                    schunk = time()
                    self.write_chunk(month_data, tmpfile)
                    tchunk = time() - schunk
                    self.logger.info('Chunk execution time: %.2f', tchunk)

                    # check everything is correct
                    filecheck = file_is_complete(tmpfile, loglevel=self.loglevel)
                    # we can later add a retry
                    if not filecheck:
                        self.logger.error('Something has gone wrong in %s!', tmpfile)
                    self.logger.info('Moving temporary file %s to %s', tmpfile, outfile)

                    move_tmp_files(self.tmpdir, self.outdir)
                del month_data
            del year_data
            if self.definitive and self.compact:
                self._concat_var_year(var, year)
        del temp_data

    def append_history(self, data):
        """
        Append comprehensive processing history to the data attributes
               
        Args:
            data: xarray Dataset or DataArray to append history to
            
        Returns:
            data: Input data with updated history attribute
        """
        history_list = ["DROP"]
        
        # Add regridding information
        if self.resolution:
            history_list.append(f"regridded from {self.reader.src_grid_name} to {self.resolution}")
        if self.frequency and self.stat:
            history_list.append(
                f"resampled from frequency {self.reader.timemodule.orig_freq} to {self.frequency} "
                f"using {self.stat} statistic")
        if self.region and self.region_name:
            region_info = f"regional selection applied ({self.region_name})"
            history_list.append(region_info)
        
        # Build the complete sentence
        if len(history_list) == 1:
            history = history_list[0]
        else:
            history = history_list[0] + ": " + ", ".join(history_list[1:])

        log_history(data, history)

        return data

    def write_chunk(self, data, outfile):
        """Write a single chunk of data - Xarray Dataset - to a specific file
        using dask if required and monitoring the progress"""

        data = self.append_history(data)

        # File to be written
        if os.path.exists(outfile):
            os.remove(outfile)
            self.logger.warning('Overwriting file %s...', outfile)

        self.logger.info("Computing to write file %s...", outfile)

        # Compute + progress monitoring
        if self.dask:
            if self.performance_reporting:
                # Full Dask dashboard to HTML
                filename = f"dask-{self.model}-{self.exp}-{self.source}-{self.nproc}.html"
                with performance_report(filename=filename):
                    job = data.persist()
                    progress(job)
                    job = job.compute()
            else:
                # Memory monitoring always on
                ms = MemorySampler()
                with ms.sample("chunk"):
                    job = data.persist()
                    progress(job)
                    job = job.compute()
                array_data = np.array(ms.samples["chunk"])
                avg_mem = np.mean(array_data[:, 1]) / 1e9
                max_mem = np.max(array_data[:, 1]) / 1e9
                self.logger.info("Avg memory used: %.2f GiB, Peak memory used: %.2f GiB", avg_mem, max_mem)
        else:
            with ProgressBar():
                job = data.compute()

        # Final safe NetCDF write (serial, no dask)
        job.to_netcdf(
            outfile,
            encoding={"time": self.time_encoding, data.name: self.var_encoding},
        )
        del job
        self.logger.info('Writing file %s successful!', outfile)

"""
DROP (Data Reduction OPerator) class

This class provides comprehensive data processing capabilities for climate datasets,
including regridding, temporal averaging, regional extraction, and archiving.
It handles multiple file formats and uses Dask for parallel processing of large datasets.

Main features:
- Regridding to arbitrary resolutions
- Temporal resampling with various statistics (mean, std, max, min, sum)
- Regional data extraction
- Automatic catalog entry generation
- Parallel processing with Dask
- Memory-efficient chunked processing
"""

import os
import shutil
from time import time

import dask
import numpy as np
import pandas as pd
from dask.distributed import Client, LocalCluster

from aqua.core.configurer import ConfigPath
from aqua.core.lock import SafeFileLock
from aqua.core.logger import log_configure, log_history
from aqua.core.reader import Reader
from aqua.core.util import dump_yaml, load_yaml
from aqua.core.util.io_util import create_folder
from aqua.core.util.string import generate_random_string

from .catalog_entry_builder import CatalogEntryBuilder
from .drop_util import move_tmp_files
from .drop_writer_netcdf import NetCDFWriter
from .drop_writer_zarr import ZarrWriter

TIME_ENCODING = {"units": "days since 1850-01-01 00:00:00", "calendar": "standard", "dtype": "float64"}

VAR_ENCODING = {"dtype": "float64", "zlib": True, "complevel": 1, "_FillValue": np.nan}

available_stats = ["mean", "std", "max", "min", "sum", "histogram"]


class Drop:
    """
    Class to generate DROP outputs at required frequency/resolution
    """

    @property
    def dask(self):
        """Check if dask is needed"""
        return self.nproc > 1

    def __init__(
        self,
        catalog=None,
        model=None,
        exp=None,
        source=None,
        var=None,
        configdir=None,
        resolution=None,
        frequency=None,
        fix=True,
        startdate=None,
        enddate=None,
        outdir=None,
        tmpdir=None,
        nproc=1,
        loglevel=None,
        region=None,
        drop=False,
        overwrite=False,
        definitive=False,
        performance_reporting=False,
        rebuild=False,
        exclude_incomplete=False,
        stat="mean",
        stat_kwargs={},
        compact="xarray",
        cdo_options=["-f", "nc4", "-z", "zip_1"],
        engine="fdb",
        output_format="netcdf",
        zarr_chunks=None,
        zarr_consolidate=False,
        **kwargs,
    ):
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
            stat (string, opt):      Statistic to compute. Can be 'mean', 'std', 'max', 'min', 'sum' or 'histogram'.
                Default is 'mean'.
            stat_kwargs (dict, opt):  kwargs to be sent to the statistic function, as 'bins' for histogram.
                Default is empty dict.
            compact (string, opt):   Compact the data into yearly files using xarray or cdo.
                                     If set to None, no compacting is performed. Default is "xarray"
            cdo_options (list, opt): List of options to be passed to cdo, default is ["-f", "nc4", "-z", "zip_1"]
            engine (string, opt):    Engine to be used by the Reader. Default is 'fdb'.
            output_format (string, opt): Output format: 'netcdf' or 'zarr'. Default is 'netcdf'.
            zarr_chunks (dict, opt): Chunk sizes for zarr (e.g. {'time': 1, 'lat': None, 'lon': None}).
                                     Default is None (uses zarr writer defaults).
            zarr_consolidate (bool, opt): Consolidate zarr metadata after writing. Default is False.
                                          Improves read performance but requires finalization step.
            **kwargs:                kwargs to be sent to the Reader, as 'zoom' or 'realization'
        """

        # Check mandatory parameters
        self.catalog = self._require_param(catalog, "catalog")
        self.model = self._require_param(model, "model")
        self.exp = self._require_param(exp, "experiment")
        self.source = self._require_param(source, "source")
        self.var = self._require_param(var, "variable string or list.")

        # General settings
        self.engine = engine
        self.logger = log_configure(loglevel, "DROP")
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
        if self.stat not in available_stats:
            raise ValueError(f"Please specify a valid statistic: {available_stats}.")
        if not isinstance(stat_kwargs, dict):
            raise TypeError("stat_kwargs must be a dictionary.")
        self.stat_kwargs = stat_kwargs

        # configure regional selection
        self._configure_region(region, drop)

        # print some info about the settings
        self._issue_info_warning()

        # define the tmpdir
        if tmpdir is None:
            self.logger.warning("No tmpdir specifield, will use outdir")
            self.tmpdir = os.path.join(outdir, "tmp")
        else:
            self.tmpdir = tmpdir
        self.tmpdir = os.path.join(self.tmpdir, f"DROP_{generate_random_string(10)}")

        # set up compacting method for concatenation
        self.compact = compact
        if self.compact not in ["xarray", "cdo", None]:
            raise KeyError("Please specify a valid compact method: xarray, cdo or None.")

        self.cdo_options = cdo_options
        if not isinstance(self.cdo_options, list):
            raise TypeError("cdo_options must be a list.")

        # configure output format and writer
        self.output_format = output_format
        if self.output_format not in ["netcdf", "zarr"]:
            raise ValueError("output_format must be 'netcdf' or 'zarr'")

        # Zarr-specific validation
        if self.output_format == "zarr":
            if self.compact is not None:
                self.logger.warning("compact option ignored for zarr output (zarr appends directly)")
                self.compact = None

        self.zarr_chunks = zarr_chunks
        self.zarr_consolidate = zarr_consolidate

        # configure the configdir
        configpath = ConfigPath(configdir=configdir)
        self.configdir = configpath.configdir

        # get default grids
        _, grids_path = configpath.get_reader_filenames()
        self.default_grids = load_yaml(os.path.join(grids_path, "default.yaml"))

        # option for encoding, defined once for all
        self.time_encoding = TIME_ENCODING
        self.var_encoding = VAR_ENCODING

        # add the performance report
        self.performance_reporting = performance_reporting

        # Create output folders
        if outdir is None:
            raise KeyError("Please specify outdir.")

        self.catbuilder = CatalogEntryBuilder(
            catalog=self.catalog,
            model=self.model,
            exp=self.exp,
            resolution=self.resolution,
            frequency=self.frequency,
            region=self.region_name,
            stat=self.stat,
            loglevel=self.loglevel,
            **self.kwargs,
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
        self.writer = None  # Will be initialized in _set_writer()

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
                raise ValueError("startdate must be a valid date string (YYYY-MM-DD or YYYYMMDD)")

        if enddate is not None:
            try:
                pd.to_datetime(enddate)
            except (ValueError, TypeError):
                raise ValueError("enddate must be a valid date string (YYYY-MM-DD or YYYYMMDD)")

    def _issue_info_warning(self):
        """
        Print information about the DROP settings
        """

        if self.startdate is not None or self.enddate is not None:
            self.logger.info("startdate is %s, enddate is %s", self.startdate, self.enddate)
            self.logger.info("startdate or enddate are set, please be sure to process one experiment at the time.")

        if not self.frequency:
            self.logger.info("Frequency not specified, no time averaging will be performed.")
        else:
            self.logger.info("Frequency: %s", self.frequency)

        if self.overwrite:
            self.logger.warning("File will be overwritten if already existing.")

        if self.exclude_incomplete:
            self.logger.info("Exclude incomplete for time averaging activated!")

        if not self.definitive:
            self.logger.warning("IMPORTANT: no file will be created, this is a dry run")

        if self.dask:
            self.logger.info("Running dask.distributed with %s workers", self.nproc)

        if self.rebuild:
            self.logger.info("rebuild=True! DROP will rebuild weights and areas!")

        self.logger.info("Variable(s) to be processed: %s", self.var)
        self.logger.info("Fixing data: %s", self.fix)
        self.logger.info("Resolution: %s", self.resolution)
        self.logger.info("Statistic to be computed: %s", self.stat)
        if self.stat_kwargs is not None and self.stat_kwargs != {}:
            self.logger.info("Additional kwargs for the statistic: %s", self.stat_kwargs)
        self.logger.info("Domain selection: %s", self.region_name)

    def _configure_region(self, region, drop):
        """Configure the region for regional selection, and the drop option"""

        if region is not None:
            self.region = region
            if self.region["name"] is None:
                raise KeyError("Please specify name in region.")
            if self.region["lon"] is None and self.region["lat"] is None:
                raise KeyError(f"Please specify at least one between lat and lon for {region['name']}.")
            self.region_name = self.region["name"]
            self.logger.info(
                "Regional selection active! region: %s, lon: %s and lat: %s...",
                self.region["name"],
                self.region["lon"],
                self.region["lat"],
            )
        else:
            self.region = None
            self.region_name = None
        self.drop = drop

    def retrieve(self):
        """
        Retrieve data from the catalog
        """

        # Initialize the reader
        self.reader = Reader(
            model=self.model,
            exp=self.exp,
            source=self.source,
            regrid=self.resolution if self.resolution != "native" else None,
            catalog=self.catalog,
            loglevel=self.loglevel,
            rebuild=self.rebuild,
            startdate=self.startdate,
            enddate=self.enddate,
            fix=self.fix,
            engine=self.engine,
            **self.kwargs,
        )

        self.logger.info("Accessing catalog for %s-%s-%s...", self.model, self.exp, self.source)

        if self.catalog is None:
            self.logger.info("Assuming catalog from the reader so that is %s", self.reader.catalog)
            self.catalog = self.reader.catalog

        self.logger.info("Retrieving data...")
        self.data = self.reader.retrieve(var=self.var)

        self.logger.debug(self.data)

    def drop_generator(self):
        """
        Generate DROP output
        """
        self.logger.info("Generating DROP output...")

        # Set up dask cluster
        self._set_dask()

        # Initialize writer after dask is set up
        self._set_writer()

        if isinstance(self.var, list):
            for var in self.var:
                self._write_var(var)

        else:  # Only one variable
            self._write_var(self.var)

        self.logger.info("Move tmp files from %s to output directory %s", self.tmpdir, self.outdir)
        # Move temporary files to output directory
        move_tmp_files(self.tmpdir, self.outdir)

        # Finalize writer (consolidate zarr metadata, etc)
        if self.writer:
            self.writer.finalize()

        # Cleaning
        self.data.close()
        self._close_dask()
        self._remove_tmpdir()

        self.logger.info("Finished generating DROP output.")

    def _define_source_grid_name(self):
        """ "
        Define the source grid name based on the resolution
        """
        if self.resolution in self.default_grids:
            return "lon-lat"
        if self.resolution == "native":
            try:
                return self.reader.source_grid_name
            except AttributeError:
                self.logger.warning("No source grid name defined in the reader, using resolution as source grid name")
                return False
        return self.resolution

    def create_catalog_entry(self):
        """
        Create an entry in the catalog for DROP
        """
        # find the catalog of my experiment and load it
        catalogfile = os.path.join(self.configdir, "catalogs", self.catalog, "catalog", self.model, self.exp + ".yaml")

        with SafeFileLock(catalogfile + ".lock", loglevel=self.loglevel):
            cat_file = load_yaml(catalogfile)

            # define the entry name
            entry_name = self.catbuilder.create_entry_name()
            sgn = self._define_source_grid_name()

            if entry_name in cat_file["sources"]:
                catblock = cat_file["sources"][entry_name]
            else:
                catblock = None

            block = self.catbuilder.create_entry_details(basedir=self.basedir, catblock=catblock, source_grid_name=sgn)

            cat_file["sources"][entry_name] = block

            # dump the update file
            dump_yaml(outfile=catalogfile, cfg=cat_file)

    def _set_writer(self):
        """
        Initialize the appropriate writer based on output_format
        """
        if self.output_format == "netcdf":
            self.writer = NetCDFWriter(
                tmpdir=self.tmpdir,
                outdir=self.outdir,
                time_encoding=self.time_encoding,
                var_encoding=self.var_encoding,
                compact=self.compact,
                cdo_options=self.cdo_options,
                dask_client=self.client,
                performance_reporting=self.performance_reporting,
                filename_builder=self.outbuilder,
                loglevel=self.loglevel,
            )
            self.logger.info("Using NetCDF writer")
        elif self.output_format == "zarr":
            self.writer = ZarrWriter(
                tmpdir=self.tmpdir,
                outdir=self.outdir,
                chunks=self.zarr_chunks,
                compressor="auto",
                consolidate=self.zarr_consolidate,
                dask_client=self.client,
                performance_reporting=self.performance_reporting,
                loglevel=self.loglevel,
            )
            self.logger.info("Using Zarr writer (consolidate=%s)", self.zarr_consolidate)

    def _set_dask(self):
        """
        Set up dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.logger.info("Setting up dask cluster with %s workers", self.nproc)
            dask.config.set({"temporary_directory": self.tmpdir})
            self.logger.info("Temporary directory: %s", self.tmpdir)
            self.cluster = LocalCluster(n_workers=self.nproc, threads_per_worker=1)
            self.client = Client(self.cluster)
        else:
            self.client = None
            dask.config.set(scheduler="synchronous")

    def _close_dask(self):
        """
        Close dask cluster
        """
        if self.dask:  # self.nproc > 1
            self.client.shutdown()
            self.cluster.close()
            self.logger.info("Dask cluster closed")

    def _remove_tmpdir(self):
        """
        Remove temporary directory
        """
        self.logger.info("Removing temporary directory %s", self.tmpdir)
        shutil.rmtree(self.tmpdir)

    def get_filename(self, var, year=None, month=None, tmp=False):
        """Create output filenames (delegates to writer)"""
        return self.writer.get_filename(var, year=year, month=month, tmp=tmp)

    def check_integrity(self, varname):
        """To check if the DROP entry is fine before running (delegates to writer)"""
        result = self.writer.check_integrity(varname, overwrite=self.overwrite)
        self.check = result["complete"]
        self.last_record = result["last_record"]

        if self.check:
            self.logger.info("Data complete for var %s...", varname)
            if self.last_record:
                self.logger.info("Last record archived is %s...", self.last_record)
        else:
            self.logger.warning("Still need to run for var %s: %s", varname, result["message"])

    def _write_var(self, var):
        """Call write var for generator or catalog access"""
        t_beg = time()

        self._write_var_catalog(var)

        t_end = time()
        self.logger.info("Process took %.4f seconds", t_end - t_beg)

    def _remove_regridded(self, data):

        # remove regridded attribute to avoid issues with Reader
        # https://github.com/oloapinivad/AQUA/issues/147
        if "AQUA_regridded" in data.attrs:
            self.logger.debug("Removing regridding attribute...")
            del data.attrs["AQUA_regridded"]
        return data

    def _write_var_catalog(self, var):
        """
        Write variable to file

        Args:
            var (str): variable name
        """

        self.logger.info("Processing variable %s...", var)
        temp_data = self.data[var]

        if self.frequency:
            # The stat_kwargs are used only if the statistic function is a callable that accepts kwargs,
            # like histogram. For other statistics, they will be ignored.
            temp_data = self.reader.timstat(
                temp_data,
                self.stat,
                freq=self.frequency,
                exclude_incomplete=self.exclude_incomplete,
                func_kwargs=self.stat_kwargs,
            )

        # temp_data could be empty after time statistics if everything was excluded
        if "time" in temp_data.coords and len(temp_data.time) == 0:
            self.logger.warning("No data available for variable %s after time statistics, skipping...", var)
            return

        # regrid
        if self.resolution and self.resolution != "native":
            temp_data = self.reader.regrid(temp_data)
            temp_data = self._remove_regridded(temp_data)

        if self.region:
            temp_data = self.reader.select_area(temp_data, lon=self.region["lon"], lat=self.region["lat"], drop=self.drop)

        # Delegate to writer with history callback
        def append_history_callback(data):
            return self.append_history(data)

        self.writer.write_variable(
            data=temp_data,
            var=var,
            overwrite=self.overwrite,
            definitive=self.definitive,
            performance_reporting=self.performance_reporting,
            history_callback=append_history_callback,
        )

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
                f"resampled from frequency {self.reader.timemodule.orig_freq} to {self.frequency} using {self.stat} statistic"
            )
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

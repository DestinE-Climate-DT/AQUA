"""An intake driver for FDB/GSV access"""
import os
import fnmatch
import datetime
import requests
import eccodes
import xarray as xr
import numpy as np
import dask
from ruamel.yaml import YAML
from intake.source import base
from aqua.core.util.eccodes import get_eccodes_attr
from aqua.core.util import to_list
from aqua.core.logger import log_configure, _check_loglevel
from .timeutil import check_dates, shift_time_dataset, floor_datetime, read_bridge_date, todatetime
from .timeutil import split_date, make_timeaxis, date2str, date2yyyymm, add_offset

# Test if FDB5 binary library is available
try:
    from gsv.retriever import GSVRetriever
    gsv_available = True
except RuntimeError:
    gsv_available = False
    gsv_error_cause = "FDB5 binary library not present on system or outdated"
except KeyError:
    gsv_available = False
    gsv_error_cause = "Environment variables for gsv, such as GRID_DEFINITION_PATH, not set."

BRIDGE_API_URL = "https://qubed.lumi.apps.dte.destination-earth.eu/api/v2/stac"  # LUMI QUBED STAC API


class GSVSource(base.DataSource):
    container = 'xarray'
    name = 'gsv'
    version = '0.0.2'
    partition_access = True

    _ds = None  # _ds and _da will contain samples of the data for dask access
    _da = None
    dask_access = False  # Flag if dask has been requested
    first_run = True  # Flag to check if this is the first run of the class

    def __init__(self, request, data_start_date, data_end_date, bridge_start_date=None, bridge_end_date=None, 
                 hpc_expver=None, timestyle="date",
                 chunks="S", savefreq="h", timestep="h", timeshift=None,
                 startdate=None, enddate=None, var=None, metadata=None, level=None,
                 switch_eccodes=False, loglevel='WARNING', engine='fdb', **kwargs):
        """
        Initializes the GSVSource class. These are typically specified in the catalog entry,
        but can also be specified upon accessing the catalog.

        Args:
            request (dict): Request dictionary
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            bridge_end_date (str, optional): End date of the bridge data. Defaults to None.
            bridge_start_date (str, optional): Start date of the bridge data. Defaults to None.
            hpc_expver (str, optional): Alternative expver to be used if the data are on hpc
            timestyle (str, optional): Time style. Defaults to "date".
            chunks (str or dict, optional): Time and vertical chunking.
                                        If a string is provided, it is assumed to be time chunking.
                                        If it is a dictionary the keys 'time' and 'vertical' are looked for.
                                        Time chunking can be one of S (step), 10M, 15M, 30M, h, 1h, 3h, 6h, D, 5D, W, M, Y.
                                        Defaults to "S".
                                        Vertical chunking is expressed as the number of vertical levels to be used.
                                        Defaults to None (no vertical chunking).
            timestep (str, optional): Time step. Can be one of 10M, 15M, 30M, 1h, h, 3h, 6h, D, 5D, W, M, Y.
                                      Defaults to "h".
            startdate (str, optional): Start date for request. Defaults to None.
            enddate (str, optional): End date for request. Defaults to None.
            var (str, optional): Variable ID. Defaults to those in the catalog.
            metadata (dict, optional): Metadata read from catalog. Contains path to FDB.
            level (int, float, list): optional level(s) to be read. Must use the same units as the original source.
            switch_eccodes (bool, optional): Flag to activate switching of eccodes path. Defaults to False.
            engine (str, optional): Engine to be used for GSV retrieval: 'polytope' or 'fdb'. Defaults to 'fdb'. 
            loglevel (string) : The loglevel for the GSVSource
            kwargs: other keyword arguments.
        """

        self.logger = log_configure(log_level=loglevel, log_name='GSVSource')
        self.engine = engine
        self.gsv_log_level = _check_loglevel(self.logger.getEffectiveLevel())
        self.logger.debug("Init of the GSV source class")

        if not gsv_available:
            raise ImportError(gsv_error_cause)

        self._request = request.copy()

        if metadata:
            self.fdbhome = metadata.get('fdb_home', None)
            self.fdbpath = metadata.get('fdb_path', None)
            self.fdbhome_bridge = metadata.get('fdb_home_bridge', None)
            self.fdbpath_bridge = metadata.get('fdb_path_bridge', None)
            if switch_eccodes:
                self.eccodes_path = metadata.get('eccodes_path', None)
                self.logger.info("ECCODES switching to %s", self.eccodes_path)
            else:
                self.logger.debug("ECCODES switching is off")
                self.eccodes_path = None
            self.levels = metadata.get('levels', None)
            self.fdb_info_file = metadata.get('fdb_info_file', None)

            # safety check for paths

            # skip the first time:
            # this is needed because intake calls initialization twice, first without custom arguments and then with
            # If we pass engine='polytope' on a remote machine the path check would fail but intake initializes the class a first time actually with the wrong engine
            if self.engine and self.engine == 'fdb' and not GSVSource.first_run:
                for attr in ['fdbhome', 'fdbpath', 'fdbhome_bridge', 
                            'fdbpath_bridge', 'eccodes_path']:
                    attr_path = getattr(self, attr)
                    if attr_path and not os.path.exists(attr_path):
                        raise FileNotFoundError(f'{attr} path {attr_path} does not exist!')

        else:
            self.fdbpath = None
            self.fdbhome = None
            self.fdbhome_bridge = None
            self.fdbpath_bridge = None
            self.fdb_info_file = None
            self.eccodes_path = None
            self.levels = None

        GSVSource.first_run = False

        # set the timestyle
        self.timestyle = timestyle
        self.timeshift = timeshift
        self.itime = 0  # position of time dim

        self.ilevel = None

        if not var:  # if no var provided keep the default in the catalog
            self._var = request["param"]
        else:
            self._var = var

        self._var = to_list(self._var)  # Make sure self._var is a list

        # Convert var names to paramId. The usage of strings is discouraged, so a warning is issued
        for i, v in enumerate(self._var):
            if isinstance(v, str):
                self.logger.warning("Variable %s is a string, conversion to paramid may lead to errors", v)
                self._var[i] = int(get_eccodes_attr(v)['paramId'])

        self.logger.debug("List of paramid to retrieve %s", self._var)

        self._kwargs = kwargs
        self.hpc_expver = hpc_expver

        # set all the start/end dates for data and bridge
        self.data_start_date = None
        self.data_end_date = None
        self.bridge_start_date = None
        self.bridge_end_date = None

        self._define_start_end_dates(data_start_date, data_end_date, bridge_start_date, bridge_end_date)
        # set all the start/end dates for the retrieval
        self._define_retrieve_dates(startdate, enddate)

        # flooring to the frequency the time to ensure that hourly, daily and monthly data
        # are read at the right time frequency
        # setting hpc and bridge availability dates
        for attr in ["data_start_date", "data_end_date", "bridge_end_date", 
                     "bridge_start_date", "startdate", "enddate"]:
            setattr(self, attr, floor_datetime(getattr(self, attr), savefreq))

        self.logger.debug('Data frequency (i.e. savefreq): %s', savefreq)
        self.logger.debug('Data_start_date: %s, Data_end_date: %s, Bridge_start_date: %s, Bridge_end_date: %s',
                          self.data_start_date, self.data_end_date, self.bridge_start_date, self.bridge_end_date)
        self.logger.debug('Request startdate: %s, Request enddate: %s', self.startdate, self.enddate)

        if self.timestyle != "yearmonth":
            offset = int(self._request.get("step", 0))  # optional initial offset for steps (in timesteps)

            # special for 6h: set offset startdate if needed
            self.startdate = add_offset(data_start_date, self.startdate, offset, timestep)

        if isinstance(chunks, dict):
            chunking_time = chunks.get('time', 'S')
            chunking_vertical = chunks.get('vertical', None)
        else:
            chunking_time = chunks
            chunking_vertical = None

        if chunking_time.upper() == "S":  # special case: time chunking is single saved frame
            chunking_time = savefreq

        self.data_startdate, self.data_starttime = split_date(self.data_start_date)

        if "levelist" in self._request:
            levelist = to_list(self._request["levelist"])
            if level:
                level = to_list(level)
                idx = list(map(levelist.index, level))
                self.idx_3d = idx
                self._request["levelist"] = level  # override default levels
                if self.levels:  # if levels in metadata select them too
                    self.levels = to_list(self.levels)
                    self.levels = [self.levels[i] for i in idx]
            else:
                self.idx_3d = list(range(0, len(levelist)))
        else:
            self.idx_3d = None

        self.onelevel = False
        if "levelist" in self._request:
            if self.levels:  # Do we have physical levels specified in metadata?
                lev = self._request["levelist"]
                if isinstance(lev, list) and len(lev) > 1:
                    self.onelevel = True  # If yes we can afford to read only one level
            else:
                self.logger.warning("A speedup of data retrieval could be achieved by specifying the levels keyword in metadata.")

        timeaxis = make_timeaxis(self.data_start_date, self.startdate, self.enddate,
                                 shiftmonth=self.timeshift, timestep=timestep,
                                 savefreq=savefreq, chunkfreq=chunking_time,
                                 bridge_start_date=self.bridge_start_date, bridge_end_date=self.bridge_end_date)

        self.timeaxis = timeaxis["timeaxis"]
        self.chk_start_idx = timeaxis["start_idx"]
        self.chk_start_date = timeaxis["start_date"]
        self.chk_end_idx = timeaxis["end_idx"]
        self.chk_end_date = timeaxis["end_date"]
        self.chk_size = timeaxis["size"]
        self._npartitions = len(self.chk_start_date)
        self.chk_type = timeaxis["type"]

        if not np.array_equal(self.chk_type, timeaxis["type_end"]):  # sanity check
            raise ValueError('Chunk size is not aligned with bridge_start_data and bridge_end_data. Fix your catalog!')
        if np.any(self.chk_type == 0):
            if not self.fdbpath and not self.fdbhome:
                raise ValueError('Some data is on HPC but no local FDB path or FDB home is specified in catalog.')
        if np.any(self.chk_type == 1):
            if not self.fdbpath_bridge and not self.fdbhome_bridge:
                raise ValueError('Some data is on bridge but no bridge FDB path or FDB home specified in catalog.')

        self.chk_vert = None
        self.ntimechunks = self._npartitions
        self.nlevelchunks = None

        if "levelist" in self._request:
            self.chunking_vertical = chunking_vertical
            if self.chunking_vertical:
                levelist = to_list(self._request["levelist"])
                if len(levelist) <= self.chunking_vertical:
                    self.chunking_vertical = None
                else:
                    self.chk_vert = [levelist[i:i+self.chunking_vertical] for i in range(0, len(levelist),
                                                                                         self.chunking_vertical)]
                    self.ntimechunks = self._npartitions
                    self.nlevelchunks = len(self.chk_vert)
                    self._npartitions = self._npartitions*len(self.chk_vert)
        else:
            self.chunking_vertical = None  # no vertical chunking

        self._switch_eccodes()

        super(GSVSource, self).__init__(metadata=metadata)

    def _define_start_end_dates(self, data_start_date, data_end_date, bridge_start_date, bridge_end_date):
        """
        Define the start and end dates of the data and bridge

        Args:
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
        """
        # Getting info from the FDB info file
        fdb_info = self._read_fdb_info()
        
        # Data dates
        self._setup_data_dates(data_start_date, data_end_date, fdb_info)
        
        # Bridge dates
        self._setup_bridge_dates(bridge_start_date, bridge_end_date, fdb_info)
        self._adjust_bridge_bounds()

    def _read_fdb_info(self):
        """
        Read FDB information from file if available
        
        Returns:
            dict or None: FDB information if available, None otherwise
        """
        if self.fdb_info_file:
            self.logger.debug('Reading FDB info from file %s', self.fdb_info_file)
            return self.get_fdb_definitions_from_file(self.fdb_info_file)
        return None

    def _setup_data_dates(self, data_start_date, data_end_date, fdb_info):
        """
        Setup data start and end dates
        
        Args:
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            fdb_info (dict or None): FDB information if available
        """
        if self.fdb_info_file and fdb_info:
            self.data_start_date = fdb_info['data']['data_start_date']
            self.data_end_date = fdb_info['data']['data_end_date']
            self.hpc_expver = fdb_info['data']['expver']
        else:
            if data_start_date == 'auto' or data_end_date == 'auto':
                self.logger.debug('Autoguessing of the FDB start and end date enabled.')
                if self.timestyle == 'yearmonth':
                    raise ValueError('Auto date selection not supported for timestyle=yearmonth. Please specify start and end date!')
                self.data_start_date, self.data_end_date = self.parse_fdb(data_start_date, data_end_date)
            else:
                self.data_start_date = data_start_date
                self.data_end_date = data_end_date

    def _setup_bridge_dates(self, bridge_start_date, bridge_end_date, fdb_info):
        """
        Setup bridge start and end dates
        
        Args:
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
            fdb_info (dict or None): FDB information if available
        """
        if self.fdb_info_file and fdb_info and fdb_info.get('bridge'):
            self._setup_bridge_dates_from_file(fdb_info)
        else:
            if bridge_start_date == 'stac' or bridge_end_date == 'stac':
                self._setup_bridge_dates_from_stac()
            else:
                self._setup_bridge_dates_from_input(bridge_start_date, bridge_end_date)

    def _setup_bridge_dates_from_file(self, fdb_info):
        """
        Setup bridge dates from FDB info file
        
        Args:
            fdb_info (dict): FDB information from file
        """
        self.bridge_start_date = fdb_info['bridge']['bridge_start_date']
        self.bridge_end_date = fdb_info['bridge']['bridge_end_date']
        self._request['expver'] = fdb_info['bridge']['expver']

    def _setup_bridge_dates_from_stac(self):
        """
        Setup bridge dates from STAC API
        """
        self.logger.debug('Reading FDB info from bridge STAC API')
        self.bridge_start_date, self.bridge_end_date = self.get_dates_from_stac_api(self._request, BRIDGE_API_URL)
        self.bridge_end_date = self.bridge_end_date + 'T2300'
        self.bridge_start_date = self.bridge_start_date + 'T0000'
        self.logger.debug('STAC API bridge start data: %s, bridge end date: %s', 
                        self.bridge_start_date, self.bridge_end_date)

    def _setup_bridge_dates_from_input(self, bridge_start_date, bridge_end_date):
        """
        Setup bridge dates from input parameters
        
        Args:
            bridge_start_date (str): Start date of the bridge data.
            bridge_end_date (str): End date of the bridge data.
        """
        # deprecated method that guess from text file and fall back
        self.bridge_start_date = read_bridge_date(bridge_start_date)
        self.bridge_end_date = read_bridge_date(bridge_end_date)

    def _adjust_bridge_bounds(self):
        """
        Adjust bridge bounds if not specified or set to 'complete'
        """
        if self.bridge_start_date == 'complete' or self.bridge_end_date == 'complete':
            self.bridge_start_date = self.data_start_date
            self.bridge_end_date = self.data_end_date
        if not self.bridge_start_date and self.bridge_end_date:
            self.bridge_start_date = self.data_start_date
        if not self.bridge_end_date and self.bridge_start_date:
            self.bridge_end_date = self.data_end_date

    def _define_retrieve_dates(self, startdate, enddate):
        """
        Define the start and end dates for the retrieval
        """

        if not startdate:
            self.startdate = self.data_start_date
        else:
            self.startdate = startdate

        if not enddate:
            self.enddate = self.data_end_date
        else:
            self.enddate = enddate

    def __getstate__(self):
        """
        This helps to pickle variables which were added later, after initialization of the class.
        These are all self variables needed by get_partition().
        """

        return {
            'data_startdate': self.data_startdate,
            'data_starttime': self.data_starttime,
            'chk_start_idx': self.chk_start_idx,
            'chk_end_idx': self.chk_end_idx,
            'chk_start_date': self.chk_start_date,
            'chk_end_date': self.chk_end_date,
            'chunking_vertical': self.chunking_vertical,
            'chk_vert': self.chk_vert,
            'chk_type': self.chk_type,
            'hpc_expver': self.hpc_expver,
            '_request': self._request,
            'timestyle': self.timestyle,
            'fdbhome': self.fdbhome,
            'fdbpath': self.fdbpath,
            'fdbhome_bridge': self.fdbhome_bridge,
            'fdbpath_bridge': self.fdbpath_bridge,
            'eccodes_path': self.eccodes_path,
            '_var': self._var,
            'timeshift': self.timeshift,
            'gsv_log_level': self.gsv_log_level,
            'logger': self.logger,
            'engine': self.engine
        }

    def __setstate__(self, state):
        """
        This helps to restore from pickle variables which were added later after initialization.
        These are all self variables needed by get_partition().
        """

        self.data_startdate = state['data_startdate']
        self.data_starttime = state['data_starttime']
        self.chk_start_idx = state['chk_start_idx']
        self.chk_end_idx = state['chk_end_idx']
        self.chk_start_date = state['chk_start_date']
        self.chk_end_date = state['chk_end_date']
        self.chunking_vertical = state['chunking_vertical']
        self.chk_vert = state['chk_vert']
        self.chk_type = state['chk_type']
        self.hpc_expver = state['hpc_expver']
        self.timestyle = state['timestyle']
        self.fdbhome = state['fdbhome']
        self.fdbpath = state['fdbpath']
        self.fdbhome_bridge = state['fdbhome_bridge']
        self.fdbpath_bridge = state['fdbpath_bridge']
        self.eccodes_path = state['eccodes_path']
        self._var = state['_var']
        self.timeshift = state['timeshift']
        self._request = state['_request']
        self.gsv_log_level = state['gsv_log_level']
        self.logger = state['logger']
        self.engine = state['engine']

    def _get_schema(self):
        """
        Standard method providing data schema.
        For dask access it is assumed that all DataArrays read share the same shape and data type.
        """

        # check if dates are within acceptable range
        check_dates(self.startdate, self.data_start_date, self.enddate, self.data_end_date)

        if self.dask_access:  # We need a better schema for dask access
            if not self._ds or not self._da:  # we still have to retrieve a sample dataset

                self._ds = self._get_partition(0, var=self._var, first=True, onelevel=self.onelevel)

                var = list(self._ds.data_vars)[0]
                da = self._ds[var]  # get first variable dataarray

                # If we have multiple levels, then this array needs to be expanded
                if self.onelevel:
                    lev = self.levels
                    apos = da.dims.index("level")  # expand the size of the "level" axis
                    attrs = da["level"].attrs
                    da = da.squeeze("level").drop_vars("level").expand_dims(level=lev, axis=apos)
                    da["level"].attrs.update(attrs)

                self._da = da

            metadata = {
                'dims': self._da.dims,
                'attrs': self._ds.attrs
            }
            schema = base.Schema(
                datashape=None,
                dtype=str(self._da.dtype),
                shape=da.shape,
                name=None,
                npartitions=self._npartitions,
                extra_metadata=metadata)
        else:
            schema = base.Schema(
                datashape=None,
                dtype=str(xr.Dataset),
                shape=None,
                name=None,
                npartitions=self._npartitions,
                extra_metadata={},
            )

        return schema

    def _switch_eccodes(self):
        """
        Internal method to switch ECCODES version if needed.
        """
        if self.eccodes_path:  # if needed switch eccodes path
            # unless we have already switched
            if self.eccodes_path and (self.eccodes_path != eccodes.codes_definition_path()):
                eccodes.codes_context_delete()  # flush old definitions in cache
                eccodes.codes_set_definitions_path(self.eccodes_path)

    def _index_to_timelevel(self, ii):
        """
        Internal method to convert partition index to time and level indices
        """
        if self.chunking_vertical:
            i = ii // len(self.chk_vert)
            j = ii % len(self.chk_vert)
        else:
            i = ii
            j = 0
        return i, j

    def _get_partition(self, ii, var=None, first=False, onelevel=False):
        """
        Standard internal method reading i-th data partition from FDB

        Args:
            ii (int): partition number
            var (string, optional): single variable to retrieve. Defaults to using those set at init
            first (bool, optional): read only the first step (used for schema retrieval)
            onelevel (bool, optional): read only one level. Defaults to False.

        Returns:
            An xarray.DataSet
        """

        request = self._request.copy()  # We are going to change it, threads do need this

        i, j = self._index_to_timelevel(ii)
        if self.chunking_vertical:
            request["levelist"] = self.chk_vert[j]

        if self.timestyle == "date":
            dds, tts = date2str(self.chk_start_date[i])
            dde, tte = date2str(self.chk_end_date[i])
            if ((dds == dde) and (tts == tte)) or first:
                request["date"] = f"{dds}"
                request["time"] = f"{tts}"
            else:
                request["date"] = f"{dds}/to/{dde}"
                request["time"] = f"{tts}/to/{tte}"

        elif self.timestyle == "step":  # style is 'step'
            request["date"] = self.data_startdate
            request["time"] = self.data_starttime
            s0 = self.chk_start_idx[i]
            s1 = self.chk_end_idx[i]
            if s0 == s1 or first:
                request["step"] = f'{s0}'
            else:
                request["step"] = f'{s0}/to/{s1}'

        elif self.timestyle == "yearmonth":  # style is 'yearmonth'
            yys, mms = date2yyyymm(self.chk_start_date[i])
            yye, mme = date2yyyymm(self.chk_end_date[i])
            if ((yys == yye) or first):
                request["year"] = f"{yys}"
            else:
                request["year"] = f"{yys}/to/{yye}"
            if ((mms == mme) or first):
                request["month"] = f"{mms}"
            else:
                request["month"] = f"{mms}/to/{mme}"
            # HACK: step is required by the code, but not needed by GSV
            # for key in ["date", "step", "time"]:
            #    if key in request:
            #        del request[key]
        else:
            raise ValueError(f'Timestyle {self.timestyle} not supported')

        if onelevel:  # limit to one single level
            request["levelist"] = request["levelist"][0]

        # If a var is used and it is a string, it means that previous parts of the code have failed
        # to convert it to paramId. The conversion is then asked to GSVRetriever, which rely on a
        # dictionary of paramId to shortName. This is not guaranteed to work, so a warning is issued.
        if var:
            if isinstance(var, str):
                self.logger.warning("Asking for variable %s as string, this may lead to errors", var)
            request["param"] = var
        else:
            request["param"] = self._var

        # Select based on type of FDB
        fstream_iterator = False  # We set it False, but it works also with True

        if self.chk_type[i]:
            # Bridge FDB type
            if self.fdbhome_bridge:
                os.environ["FDB_HOME"] = self.fdbhome_bridge
                self.logger.debug('Access is BRIDGE and FDB_HOME is set to %s', self.fdbhome_bridge)
            if self.fdbpath_bridge:
                os.environ["FDB5_CONFIG_FILE"] = self.fdbpath_bridge
                self.logger.debug('Access is BRIDGE and FDB5_CONFIG_FILE is set to %s', self.fdbpath_bridge)
            fstream_iterator = True
        else:
            # HPC FDB type
            if self.fdbhome:  # if fdbhome is provided, use it, since we are creating a new gsv
                os.environ["FDB_HOME"] = self.fdbhome
                self.logger.debug('Access is HPC and FDB_HOME is set to %s', self.fdbhome)
            if self.fdbpath:  # if fdbpath provided, use it, since we are creating a new gsv
                os.environ["FDB5_CONFIG_FILE"] = self.fdbpath
                self.logger.debug('Access is HPC and FDB5_CONFIG_FILE is set to %s', self.fdbpath)
            if self.hpc_expver:
                request["expver"] = self.hpc_expver

        self._switch_eccodes()

        # The following is a hack around a pyfdb/fdb5 bug which requires a double initialization when reading from bridge
        # See https://github.com/DestinE-Climate-DT/AQUA/issues/1715
        # Notice also that for some mysterious reason this works only if the result is stored in self (even if then it is not used)
        if self.chk_type[i]:
            self.gsv = GSVRetriever(engine=self.engine, logging_level=self.gsv_log_level)
        gsv = GSVRetriever(engine=self.engine, logging_level=self.gsv_log_level)

        self.logger.debug('Request %s', request)
        dataset = gsv.request_data(request, use_stream_iterator=fstream_iterator,
                                   process_derived_variables=False)  # following 2.9.2 we avoid derived variables

        if self.timeshift:  # shift time by one month (special case)
            dataset = shift_time_dataset(dataset)

        return dataset

    def read(self):
        """Return a in-memory dask dataset"""
        ds = [self._get_partition(i) for i in range(self._npartitions)]
        ds = xr.concat(ds, dim='time', coords='different')
        return ds

    def get_part_delayed(self, ii, var, shape, dtype):
        """
        Function to read a delayed partition.
        Returns a dask.array

        Args:
            ii (int): partition number
            var (string): variable name
            shape: shape of the schema
            dtype: data type of the schema
        """

        i, j = self._index_to_timelevel(ii)

        ds = dask.delayed(self._get_partition)(ii, var=var)

        # get the data from the first (and only) data array
        ds = ds.to_array()[0].data
        newshape = list(shape)
        newshape[self.itime] = self.chk_size[i]
        if self.chunking_vertical:  # if we have vertical chunking
            newshape[self.ilevel] = len(self.chk_vert[j])

        return dask.array.from_delayed(ds, newshape, dtype)

    def to_dask(self):
        """Return a dask xarray dataset for this data source"""

        self.dask_access = True  # This is used to tell _get_schema() to load dask info
        self._load_metadata()

        shape = self._schema.shape
        dtype = self._schema.dtype

        self.itime = self._da.dims.index("time")
        if self.chunking_vertical:
            self.ilevel = self._da.dims.index("level")

        if 'valid_time' in self._da.coords:  # temporary hack because valid_time is inconsistent anyway
            self._da = self._da.drop_vars('valid_time')

        coords = self._da.coords.copy()
        coords['time'] = self.timeaxis

        ds = xr.Dataset()

        # Now works only with the variables which have been read (the fixer may change names later)
        # Notice that the mismatch between shortnames in different versions of eccodes is handled here
        # We consider stable between versions the paramId, not the shortName. This means that we read
        # the GRIB_paramid attribute and based on this we get the shortName from the eccodes definitions.
        # If you want to read the shortName according to previous versions of eccodes, you need to
        # set the switch_eccodes flag to True in the catalog.
        for var in self._ds.data_vars:
            # We need to ask for the GRIB_paramid that is attribute of the variable
            original_paramid = self._ds[var].attrs.get("GRIB_paramId", var)
            updated_var = get_eccodes_attr(original_paramid)['shortName']
            # If this is executed, it means that the shortname came from a previous version of eccodes
            # and we're using a more recent one in which the paramId is still existing, but the shortName
            # has changed. This is a warning to the user. However the final variable name will be influenced
            # by this only if fix=False.
            if updated_var != var:
                self.logger.warning("Variable shortname %s has been interpreted with another eccodes. Current eccodes %s will read paramid %s as %s", var, eccodes.__version__, original_paramid, updated_var)
            # Create a dask array from a list of delayed get_partition calls
            if not self.chunking_vertical:
                dalist = [self.get_part_delayed(i, original_paramid, shape, dtype) for i in range(self.npartitions)]
                darr = dask.array.concatenate(dalist, axis=self.itime)  # This is a lazy dask array
            else:
                dalist = []
                for j in range(self.nlevelchunks):
                    dalistlev = [self.get_part_delayed(i*self.nlevelchunks+j, original_paramid, shape, dtype) for i in range(self.ntimechunks)]
                    dalist.append(dask.array.concatenate(dalistlev, axis=self.itime))
                darr = dask.array.concatenate(dalist, axis=self.ilevel)  # This is a lazy dask array

            da = xr.DataArray(darr,
                              name=updated_var,
                              attrs=self._ds[var].attrs, # We need the original var to retrieve the attributes
                              dims=self._da.dims,
                              coords=coords)

            log_history(da, "Dataset retrieved by GSV interface")

            ds[updated_var] = da

        ds.attrs.update(self._ds.attrs)
        if self.idx_3d:
            ds = ds.assign_coords(idx_level=("level", self.idx_3d))

        return ds

    # Overload read_chunked() from base.DataSource
    def read_chunked(self):
        """Return iterator over container fragments of data source"""
        self._load_metadata()
        for i in range(self.npartitions):
            ds = self._get_partition(i)
            if self.idx_3d:
                ds = ds.assign_coords(idx_level=("level", self.idx_3d))
            yield ds

    def get_fdb_definitions_from_file(self, fdb_info_file):
        """
        Get the FDB definitions from a file

        Args:
            file (str): path to the file

        Returns:
            dict: definitions
        """
        if not os.path.exists(fdb_info_file):
            self.logger.error("FDB info file %s does not exist", fdb_info_file)
            return None

        yaml = YAML()

        try:
            with open(fdb_info_file, 'r') as file:
                fdb_info = yaml.load(file)
        except (OSError, yaml.YAMLError) as e:
            self.logger.error("Error reading or parsing YAML file %s: %s", fdb_info_file, str(e))
            return None

        # The 'data' block is mandatory and present since the first chunck of simulation
        # The 'bridge' block is written only if some data is on bridge (see #1760)
        if 'data' in fdb_info:
            try:
                fdb_info['data']['data_start_date'] = self._validate_info_date(fdb_info, 'data', 'start')
                fdb_info['data']['data_end_date'] = self._validate_info_date(fdb_info, 'data', 'end')
            except KeyError:
                self.logger.error("FDB info file %s does not contain HPC dates in correct format", fdb_info_file)
                return None
        else:
            self.logger.error("FDB info file %s does not contain 'data' section, which is mandatory", fdb_info_file)
            return None
        if 'bridge' in fdb_info and (self.fdbhome_bridge or self.fdbpath_bridge):
            try:
                fdb_info['bridge']['bridge_start_date'] = self._validate_info_date(fdb_info, 'bridge', 'start')
                fdb_info['bridge']['bridge_end_date'] = self._validate_info_date(fdb_info, 'bridge', 'end')
            # if bridge dates are wrongly defined, set the bridge block to None
            except KeyError:
                self.logger.error("FDB info file %s does not contain bridge dates in correct form", fdb_info_file)
                fdb_info['bridge'] = None
        else:
            fdb_info['bridge'] = None

        return fdb_info

    @staticmethod
    def _validate_info_date(fdb_info_file, location='data', kind='start'):

        if location not in ['data', 'bridge']:
            raise ValueError(f'location {location} should be either data or local')

        if kind not in ['start', 'end']:
            raise ValueError(f'kind {kind} should be either start or end')

        return todatetime(fdb_info_file[location][f'{location}_{kind}_date']).strftime('%Y%m%dT%H%M')

    def parse_fdb(self, start_date, end_date):
        """
        Parse the FDB config file and return the start and end dates of the data.
        This works only with the DE GSV schema.

        Args:
            start_date (str): if 'auto' the start date is found automatically. Else it is the start date.
            end_date (str): if 'auto' the end date is found automatically. Else it is the end date.

        Returns:
            tuple: start and end dates
        """
        if not self.fdbhome and not self.fdbpath:
            raise ValueError('Automatic dates requested but no FDB home or FDB path specified in catalog.')

        yaml = YAML()

        if self.fdbhome:  # FDB_HOME takes precedence but assumes a fixed subdirectory structure
            yamlfile = os.path.join(self.fdbhome, 'etc/fdb/config.yaml')
        else:
            yamlfile = self.fdbpath

        with open(yamlfile, 'r') as file:
            cfg = yaml.load(file)

        if 'fdbs' in cfg:
            root = cfg['fdbs'][0]['spaces'][0]['roots'][0]['path']
        else:
            root = cfg['spaces'][0]['roots'][0]['path']

        req = self._request
        expver = req['expver']
        if self.hpc_expver:
            expver = self.hpc_expver

        file_mask = f"{req['class']}:{req['dataset']}:{req['activity']}:{req['experiment']}:{req['generation']}:{req['model']}:{req['realization']}:{expver}:{req['stream']}:*"

        file_mask = file_mask.lower()
        file_list = [
            f for f in os.listdir(root) if fnmatch.fnmatch(f.lower(), file_mask)
        ]

        datesel = [filename[-8:] for filename in file_list if (filename[-8:].isdigit() and len(filename[-8:]) == 8)]
        datesel.sort()

        if len(datesel) == 0:
            raise ValueError('Auto date selection in catalog but no valid dates found in FDB')
        else:
            if start_date == 'auto':
                start_date = datesel[0] + 'T0000'
            if end_date == 'auto':
                end_date = datesel[-2] + 'T2300'
            self.logger.info('Automatic FDB date range: %s - %s', start_date, end_date)

        return start_date, end_date
    
    @staticmethod
    def get_dates_from_stac_api(params, base_url=BRIDGE_API_URL):
        """
        Function to get from the STAC data bridge the available
        dates of a dataset on the bridge
        
        Args:
            params (dict): Dictionary of parameters to interrogate the STAC API.
                        In principle, the same as the usual FDB request
            base_url (str): URL for the STAC API

        Returns:
            tuple: A tuple containing the start and end dates of the dataset
        """

        # Define the base URL for the STAC API
        params['root'] = 'root'
        params['param'] = to_list(params['param'])[0]
        for p in ['date', 'time', 'step', 'year', 'month']:
            params.pop(p, None)  # remove date/time/step/year/month if present

        # new stac API requires lowercased keys
        params = {k: v.lower() if isinstance(v, str) else v for k, v in params.items()}

        # network problems can happen, so we need to handle them
        try:
            response = requests.get(base_url, params=params, timeout=10)
        except requests.Timeout as e:
            raise TimeoutError("STAC API request timed out after 10 seconds.") from e
        except requests.RequestException as e:
            raise ConnectionError("STAC API request failed") from e

        # Check the response status code
        if response.status_code == 400:
            raise ValueError(f"Bad request to STAC API: {response.text}")
        if response.status_code == 503:
            raise ValueError(f"Service unavailable: {response.text}")
        if response.status_code != 200:
            raise ValueError(f"Unexpected response from STAC API: {response.status_code} - {response.text}")

        # parse the JSON response
        try:
            stac_json = response.json()
        except ValueError as exc:
            raise ValueError("Failed to parse STAC API response as JSON") from exc

    
        dateblock = [el for el in stac_json['links'] if el.get('title') == 'date']
        if not dateblock:
            raise ValueError(f"The first link in the response is not a date link, but {dateblock}")

        # specific extraction of the dates: new format following the qube STAC API
        dates = dateblock[0].get('variables').get('date').get('enum')
        sorted_dates = sorted(dates)
        
        return sorted_dates[0], sorted_dates[-1]


# This function is repeated here in order not to create a cross dependency between GSVSource and AQUA
def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})

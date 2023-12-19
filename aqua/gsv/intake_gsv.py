"""An intake driver for FDB/GSV access"""
import os
import datetime
import sys
import eccodes
import xarray as xr
import dask
from aqua.util.eccodes import init_get_eccodes_shortname
from intake.source import base
from .timeutil import check_dates, shift_time_dataset
from .timeutil import split_date, make_timeaxis, date2str, add_offset
from aqua.logger import log_configure, _check_loglevel

# Test if FDB5 binary library is available
try:
    from gsv.retriever import GSVRetriever
    gsv_available = True
except RuntimeError:
    gsv_available = False
    gsv_error_cause = "FDB5 binary library not present on system on outdated"
except KeyError:
    gsv_available = False
    gsv_error_cause = "Environment variables for gsv, such as GRID_DEFINITION_PATH, not set."


class GSVSource(base.DataSource):
    container = 'xarray'
    name = 'gsv'
    version = '0.0.2'
    partition_access = True
    timeaxis = None
    chk_start_idx = None
    chk_start_date = None
    chk_end_idx = None
    chk_end_date = None
    chk_size = None

    _ds = None  # This will contain a sample of the data for dask access
    dask_access = False  # Flag if dask has been requested
    timeaxis = None  # Used for dask access

    def __init__(self, request, data_start_date, data_end_date, timestyle="date",
                 aggregation="S", savefreq="H", timestep="H", timeshift=None,
                 startdate=None, enddate=None, var=None, metadata=None,
                 loglevel='WARNING', **kwargs):
        """
        Initializes the GSVSource class. These are typically specified in the catalogue entry,
        but can also be specified upon accessing the catalogue.

        Args:
            request (dict): Request dictionary
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            timestyle (str, optional): Time style. Defaults to "date".
            aggregation (str, optional): Time aggregation level.
                                         Can be one of S (step), 10M, 15M, 30M, 1H, H, 3H, 6H, D, 5D, W, M, Y.
                                         Defaults to "S".
            timestep (str, optional): Time step. Can be one of 10M, 15M, 30M, 1H, H, 3H, 6H, D, 5D, W, M, Y.
                                      Defaults to "H".
            startdate (str, optional): Start date for request. Defaults to None.
            enddate (str, optional): End date for request. Defaults to None.
            var (str, optional): Variable ID. Defaults to those in the catalogue.
            metadata (dict, optional): Metadata read from catalogue. Contains path to FDB.
            loglevel (string) : The loglevel for the GSVSource
            kwargs: other keyword arguments.
        """

        self.logger = log_configure(log_level=loglevel, log_name='GSVSource')

        if not gsv_available:
            raise ImportError(gsv_error_cause)

        if metadata:
            self.fdbpath = metadata.get('fdb_path', None)
            self.eccodes_path = metadata.get('eccodes_path', None)
        else:
            self.fdbpath = None
            self.eccodes_path = None

        if not startdate:
            startdate = data_start_date
        if not enddate:
            enddate = data_end_date

        offset = int(request["step"])  # optional initial offset for steps (in timesteps)

        # special for 6h: set offset startdate if needed
        startdate = add_offset(data_start_date, startdate, offset, timestep)

        self.timestyle = timestyle

        if aggregation.upper() == "S":  # special case: 'aggegation at single saved level
            aggregation = savefreq

        self.timeshift = timeshift
        self.itime = 0  # position of time dim

        self.data_startdate, self.data_starttime = split_date(data_start_date)

        if not var:  # if no var provided keep the default in the catalogue
            self._var = request["param"]
        else:
            self._var = var

        self._request = request.copy()
        self._kwargs = kwargs

        sys._gsv_work_counter = 0  # used to suppress printing

        self.get_eccodes_shortname = init_get_eccodes_shortname()

        self.data_start_date = data_start_date
        self.data_end_date = data_end_date
        self.startdate = startdate
        self.enddate = enddate

        (self.timeaxis, self.chk_start_idx,
         self.chk_start_date, self.chk_end_idx,
         self.chk_end_date, self.chk_size) = make_timeaxis(self.data_start_date, self.startdate, self.enddate,
                                                           shiftmonth=self.timeshift, timestep=timestep,
                                                           savefreq=savefreq, chunkfreq=aggregation)

        self._npartitions = len(self.chk_start_date)

        super(GSVSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        """
        Standard method providing data schema.
        For dask access it is assumed that all dataarrays read share the same shape and data type.
        """

        # check if dates are within acceptable range
        check_dates(self.startdate, self.data_start_date, self.enddate, self.data_end_date)

        if self.dask_access:  # We need a better schema for dask access
            if not self._ds:  # we still have to retrieve a sample dataset
                self._ds = self._get_partition(0, var=self._var, first=True)

            var = list(self._ds.data_vars)[0]
            da = self._ds[var]  # get first variable dataarray

            metadata = {
                'dims': da.dims,
                'attrs': self._ds.attrs
            }
            schema = base.Schema(
                datashape=None,
                dtype=str(da.dtype),
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

    def _get_partition(self, i, var=None, first=False, dask=False):
        """
        Standard internal method reading i-th data partition from FDB
        Args:
            i (int): partition number
            var (string, optional): single variable to retrieve. Defaults to using those set at init
            first (bool, optional): read only the first step (used for schema retrieval)
        Returns:
            An xarray.DataSet
        """

        request = self._request.copy()  # We are going to change it, threads do need this

        if self.timestyle == "date":
            dds, tts = date2str(self.chk_start_date[i])
            dde, tte = date2str(self.chk_end_date[i])
            request["date"] = f"{dds}/to/{dde}"
            request["time"] = f"{tts}/to/{tte}"
            s0 = None
            s1 = None
        else:  # style is 'step'
            request["date"] = self.data_startdate
            request["time"] = self.data_starttime

            s0 = self.chk_start_idx[i]
            s1 = self.chk_end_idx[i]

            if s0 == s1 or first:
                request["step"] = f'{s0}'
            else:
                request["step"] = f'{s0}/to/{s1}'

        if var:
            request["param"] = var
        else:
            request["param"] = self._var

        if self.fdbpath:  # if fdbpath provided, use it, since we are creating a new gsv
            os.environ["FDB5_CONFIG_FILE"] = self.fdbpath

        if self.eccodes_path:  # if needed switch eccodes path
            # unless we have already switched
            if self.eccodes_path and (self.eccodes_path != eccodes.codes_definition_path()):
                eccodes.codes_context_delete()  # flush old definitions in cache
                eccodes.codes_set_definitions_path(self.eccodes_path)

        # for some reason this is needed here and not in init
        gsv_log_level = _check_loglevel(self.logger.getEffectiveLevel())
        gsv = GSVRetriever(logging_level=gsv_log_level)

        # if self.verbose:
        #     print("Request: ", i, self._var, s0, s1, request)
        #     dataset = gsv.request_data(request)
        # else:
        #     with NoPrinting():
        #         dataset = gsv.request_data(request)

        # to silence the logging from the GSV retriever, we increase its level by one
        # in this way the 'info' is printed only in 'debug' mode
        # gsv_log_level = _check_loglevel(self.logger.getEffectiveLevel() + 10)
        self.logger.debug('Request %s', request)
        dataset = gsv.request_data(request)

        if self.timeshift:  # shift time by one month (special case)
            dataset = shift_time_dataset(dataset)

        # Log history
        log_history(dataset, "Dataset retrieved by GSV interface")

        return dataset

    def read(self):
        """Return a in-memory dask dataset"""
        ds = [self._get_partition(i) for i in range(self._npartitions)]
        ds = xr.concat(ds, dim='time')
        return ds

    def get_part_delayed(self, i, var, shape, dtype):
        """
        Function to read a delayed partition.
        Returns a dask.array

        Args:
            i (int): partition number
            var (string): variable name
            shape: shape of the schema
            dtype: data type of the schema
        """
        ds = dask.delayed(self._get_partition)(i, var=var, dask=True)

        # get the data from the first (and only) data array
        ds = ds.to_array()[0].data
        newshape = list(shape)
        newshape[self.itime] = self.chk_size[i]
        return dask.array.from_delayed(ds, newshape, dtype)

    def to_dask(self):
        """Return a dask xarray dataset for this data source"""

        self.dask_access = True  # This is used to tell _get_schema() to load dask info
        self._load_metadata()

        shape = self._schema.shape
        dtype = self._schema.dtype

        var = list(self._ds.data_vars)[0]
        da0 = self._ds[var]  # sample dataarray

        self.itime = da0.dims.index("time")
        coords = da0.coords.copy()
        coords['time'] = self.timeaxis

        ds = xr.Dataset()

        for var in self._var:
            # Create a dask array from a list of delayed get_partition calls
            dalist = [self.get_part_delayed(i, var, shape, dtype) for i in range(self.npartitions)]
            darr = dask.array.concatenate(dalist, axis=self.itime)  # This is a lazy dask array

            shortname = self.get_eccodes_shortname(var)

            da = xr.DataArray(darr,
                              name=shortname,
                              attrs=self._ds[shortname].attrs,
                              dims=self._ds[shortname].dims,
                              coords=coords)

            
            ds[shortname] = da

        ds.attrs.update(self._ds.attrs)

        return ds


# class NoPrinting:
#     """
#     Context manager to suppress printing
#     """

#     def __enter__(self):
#         sys._gsv_work_counter += 1
#         if sys._gsv_work_counter == 1 and not isinstance(sys.stdout, io.StringIO):  # We are really the first
#             sys._org_stdout = sys.stdout  # Record the original in sys
#             self._trap = io.StringIO()
#             sys.stdout = self._trap

#     def __exit__(self, exc_type, exc_val, exc_tb):
#         sys._gsv_work_counter -= 1
#         if sys._gsv_work_counter == 0:  # We are really the last one
#             sys.stdout = sys._org_stdout  # Restore the original


# This function is repeated here in order not to create a cross dependency between GSVSource and AQUA
def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})

"""An intake driver for FDB/GSV access"""

import datetime
import sys
import io
import xarray as xr
import dask
from intake.source import base
from .timeutil import check_dates, shift_time_dataset
from .timeutil import split_date, make_timeaxis, date2str, add_offset

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

    _ds = None  # This will contain a sample of the data for dask access
    dask_access = False  # Flag if dask has been requested
    timeaxis = None  # Used for dask access

    def __init__(self, request, data_start_date, data_end_date, timestyle="date",
                 aggregation="S", savefreq="H", timestep="H", timeshift=None,
                 startdate=None, enddate=None, var='167', metadata=None, verbose=False, **kwargs):
        """
        Initializes the GSVSource class. These are typically specified in the catalogue entry, but can also be specified upon accessing the catalogue.

        Args:
            request (dict): Request dictionary
            data_start_date (str): Start date of the available data.
            data_end_date (str): End date of the available data.
            timestyle (str, optional): Time style. Defaults to "date".
            aggregation (str, optional): Time aggregation level. Can be one of S (step), 10M, 15M, 30M, 1H, H, 3H, 6H, D, 5D, W, M, Y. Defaults to "S". 
            timestep (str, optional): Time step. Can be one of 10M, 15M, 30M, 1H, H, 3H, 6H, D, 5D, W, M, Y. Defaults to "H". 
            startdate (str, optional): Start date for request. Defaults to None.
            enddate (str, optional): End date for request. Defaults to None.
            var (str, optional): Variable ID. Defaults to "167".
            metadata (dict, optional): Metadata read from catalogue. Contains path to FDB.
            verbose (bool, optional): Whether to print additional info to screen. Used only for FDB access. Defaults to False.
            kwargs: other keyword arguments.
        """

        if gsv_available:
            self.gsv = GSVRetriever()
        else:
            raise ImportError(gsv_error_cause)
                
        if not startdate:
            startdate = data_start_date
        if not enddate:
            enddate = data_end_date

        offset = int(request["step"])  # optional initial offset for steps (in timesteps)

        startdate = add_offset(data_start_date, startdate, offset, timestep)  # special for 6h: set offset startdate if needed

        # check if dates are within acceptable range
        check_dates(startdate, data_start_date, enddate, data_end_date)
        
        self.timestyle = timestyle

        if aggregation.upper() == "S":  # special case: 'aggegation at single saved level
            aggregation = savefreq

        self.timeshift = timeshift
        self.itime = 0  # position of time dim

        self.data_startdate, self.data_starttime = split_date(data_start_date)

        self._var = var
        self.verbose = verbose

        self._request = request.copy()
        self._kwargs = kwargs

        sys._gsv_work_counter = 0  # used to suppress printing

        (self.timeaxis, self.chk_start_idx,
         self.chk_start_date, self.chk_end_idx,
         self.chk_end_date, self.chk_size) = make_timeaxis(data_start_date, startdate, enddate,
                                                           shiftmonth=timeshift, timestep=timestep,
                                                           savefreq=savefreq, chunkfreq=aggregation)

        self._npartitions = len(self.chk_start_date)

        super(GSVSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        """Standard method providing data schema"""

        if self.dask_access:  # We need a better schema for dask access
            if not self._ds:  # we still have to retrieve a sample dataset
                self._ds = self._get_partition(0, first=False)
            
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
                name=var,
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

    def _get_partition(self, i, first=False):
        """
        Standard internal method reading i-th data partition from FDB
        Args:
            i (int): partition number
            first (boo): read only the first step (used for schema retrieval)
        Returns:
            An xarray.DataSet
        """

        request = self._request.copy()  # We are going to modify it

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

        if self._var:  # if no var provided keep the default in the catalogue
            request["param"] = self._var

        if self.dask_access:  # if we are using dask then each get_partition needs its own gsv instance.
                              # Actually it is needed for each processor, this could be possibly improved
            gsv = GSVRetriever() 
        else:
            gsv = self.gsv  # use the one which we already created

        # if self.verbose:
        #     print("Request: ", i, self._var, s0, s1, request)
        # else:  # suppress printing
        #     self.nwork += 1  # increment number of active reads
        #     print(i, self.nwork, "active")
        #     if not self.stdout:  # am I the first one to do this?
        #         self.stdout = sys.stdout
        #         #self.text_trap = io.StringIO()
        #         #sys.stdout = self.text_trap
        if self.verbose:
            print("Request: ", i, self._var, s0, s1, request)
            dataset = gsv.request_data(request)
        else:
            with NoPrinting():
                dataset = gsv.request_data(request)

        if self.timeshift:  # shift time by one month (special case)
            dataset = shift_time_dataset(dataset)

        # Fix GRIB attribute names. This removes "GRIB_" from the beginning
        for var in dataset.data_vars:
            dataset[var].attrs = {key.split("GRIB_")[-1]: value for key, value in dataset[var].attrs.items()}

        # Log history
        log_history(dataset, "dataset retrieved by GSV interface")

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
        """
        ds = dask.delayed(self._get_partition)(i)[var].data
        newshape = list(shape)
        newshape[self.itime] = self.chk_size[i]
        return dask.array.from_delayed(ds, newshape, dtype)


    def to_dask(self):
        """Return a dask xarray dataset for this data source"""

        self.dask_access = True  # This is used to tell _get_schema() to load dask info
        self._load_metadata()

        var = self._schema.name
        shape = self._schema.shape
        dtype = self._schema.dtype

        da0 = self._ds[var]  # sample dataarray

        self.itime = da0.dims.index("time")

        # Create a dask array from a list of delayed get_partition calls
        dalist = [self.get_part_delayed(i, var, shape, dtype) for i in range(self.npartitions)]
        darr = dask.array.concatenate(dalist, axis=self.itime)  # This is a lazy dask array

        coords = da0.coords.copy()
        coords['time'] = self.timeaxis

        da = xr.DataArray(darr,
                  name = da0.name,
                  attrs = da0.attrs,
                  dims = da0.dims,
                  coords = coords)

        ds = da.to_dataset()
        ds.attrs.update(self._ds.attrs)

        return ds


class NoPrinting:
    """
    Context manager to suppress printing
    """

    def __enter__(self):
        sys._gsv_work_counter += 1
        if sys._gsv_work_counter == 1 and not isinstance(sys.stdout, io.StringIO):  # We are really the first
             sys._org_stdout = sys.stdout  # Record the original in sys
             self._trap = io.StringIO()
             sys.stdout = self._trap

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys._gsv_work_counter -= 1
        if sys._gsv_work_counter == 0:  # We are really the last one
             sys.stdout = sys._org_stdout  # Restore the original


# This function is repeated here in order not to create a cross dependency between GSVSource and AQUA
def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})

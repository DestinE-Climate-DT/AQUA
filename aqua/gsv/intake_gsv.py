"""An intake driver for FDB/GSV access"""

import sys
import os
import contextlib
import datetime
import xarray as xr
from intake.source import base
from .timeutil import compute_date_steps, compute_date, check_dates, compute_mars_timerange
from .timeutil import shift_time_dataset, shift_time_datetime, compute_steprange, dateobj, set_stepmin, split_date

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

    def __init__(self, request, data_start_date, data_end_date, timestyle="date",
                 aggregation="S", timestep="H", timeshift=None,
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
        
        if not startdate:
            startdate = data_start_date
        if not enddate:
            enddate = data_end_date
        
        check_dates(startdate, data_start_date, enddate, data_end_date)

        self.timestyle = timestyle

        if aggregation.upper() == "S":  # special case: 'aggegation at single step level
            aggregation = timestep

        self.aggregation = aggregation
        self.timestep = timestep
        self.timeshift = timeshift

        self.data_startdate, self.data_starttime = split_date(data_start_date)
        self.startdate, self.starttime = split_date(startdate)
        self.enddate, self.endtime = split_date(enddate)

        self.stepmin = request["step"]  # The minimum existing timestep 
        self._var = var
        self.verbose = verbose

        self.request = request
        self._kwargs = kwargs

        if timestyle == "step" and self.stepmin > 0:  # make sure that we start retrieving data from the first available step
            self.startdate, self.starttime = set_stepmin(self.startdate, self.starttime,
                                                         self.data_startdate, self.data_starttime,
                                                         self.stepmin, self.timestep)

        self._npartitions = compute_date_steps(self.startdate, self.enddate, aggregation,
                                                   starttime=self.starttime, endtime=self.endtime)

        if gsv_available:
            self.gsv = GSVRetriever()
        else:
            raise ImportError(gsv_error_cause)
        self._dataset = None
        super(GSVSource, self).__init__(metadata=metadata)

    def _get_schema(self):
        """Standard method providing data schema"""

        return base.Schema(
            datashape=None,
            dtype=xr.Dataset,
            # shape=self._dataset.shape,
            shape=None,
            npartitions=self._npartitions,
            extra_metadata={},
        )

    def _get_partition(self, i):
        """Standard internal method reading i-th data partition from FDB"""

        if self.timestyle == "date":
            dd, tt, _ = compute_date(self.startdate, self.starttime, self.aggregation, i, self._npartitions)
            dform, tform = compute_mars_timerange(dd, tt, self.aggregation, self.timestep)  # computes date and time range in mars style
            self.request["date"] = dform
            self.request["time"] = tform
        else:  # style is 'step'
            self.request["date"] = self.data_startdate
            self.request["time"] = self.data_starttime

            date0 = dateobj(self.data_startdate, self.data_starttime)

            _, _, ndate0 = compute_date(self.startdate, self.starttime, self.aggregation, i, self._npartitions)
            _, _, ndate1 = compute_date(self.startdate, self.starttime, self.aggregation, i + 1, self._npartitions)

            if self.timeshift:  # shift time for selection by given amount (needed eg. by GSV monthly)
                ndate0 = shift_time_datetime(ndate0, self.timeshift, sign=-1)
                ndate1 = shift_time_datetime(ndate1, self.timeshift, sign=-1)

            s0 = compute_steprange(date0, ndate0, self.timestep)
            s1 = compute_steprange(date0, ndate1, self.timestep) - 1

            if s0 == s1:
                self.request["step"] = f'{s0}'
            else:
                self.request["step"] = f'{s0}/to/{s1}'

        if self._var:  # if no var provided keep the default in the catalogue
            self.request["param"] = self._var

        if self.verbose:
            print("Request: ", self.request)
            dataset = self.gsv.request_data(self.request)
        else:
            with open(os.devnull, "w") as f, contextlib.redirect_stdout(f):
                dataset = self.gsv.request_data(self.request)

        if self.timeshift:  # shift time by given amount (needed eg. by GSV monthly)
            dataset = shift_time_dataset(dataset, self.timeshift)

        # Fix GRIB attribute names. This removes "GRIB_" from the beginning
        for var in dataset.data_vars:
            dataset[var].attrs = {key.split("GRIB_")[-1]: value for key, value in dataset[var].attrs.items()}

        # Log history
        log_history(dataset, "dataset retrieved by GSV interface")

        return dataset

    def read(self):
        """Public method providing iterator to read the data"""
        ds = [self._get_partition(i) for i in range(self._npartitions)]
        ds = xr.concat(ds, dim='time')
        return ds

    def to_dask(self):
        return self.read_chunked()

    # def _load(self):
    #     self._dataset = self._get_partition(0)


# This function is repeated here in order not to create a cross dependency between GSVSource and AQUA

def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})

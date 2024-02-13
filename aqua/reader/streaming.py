"""Streaming Mixin for Reader"""

import pandas as pd
import numpy as np
#from aqua.logger import log_configure
from aqua.util import frequency_string_to_pandas
from aqua.util import extract_literal_and_numeric


class Streaming():
    """Streaming class to be used in Reader and elsewhere"""

    def __init__(self, aggregation='S', startdate=None, enddate=None, loglevel=None):
        """
        The Streaming constructor.
        The streamer  is used to stream data by either a specific time interval
        or by a specific number of samples. If the unit parameter is specified, the
        data is streamed by the specified unit and stream_step (e.g. 1 month).
        If the unit parameter is not specified, the data is streamed by stream_step
        steps of the original time resolution of input data.

        If the stream function is called a second time, it will return the subsequent
        chunk of data in the sequence. The function keeps track of the state of the
        streaming process through the use of an internal counter.
        This allows the user to stream through the entire dataset in multiple calls to the function,
        retrieving consecutive chunks of data each time.

        If startdate is not specified, the method will use the first date in the dataset.

        Arguments:
            startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            enddate (str): the ending date for streaming the data (e.g. '2021-01-01') (None)
            aggregation (str): the streaming frequency in pandas style (1M, 7D etc.)
            loglevel (string):      Level of logging according to logging module
                                    (default: log_level_default of loglevel())

        Returns:
            A `Streaming` class object.
        """

        # define the internal logger
        # self.logger = log_configure(log_level=loglevel, log_name='Streaming')
        self.startdate = startdate
        self.enddate = enddate
        self.aggregation = aggregation
        self.idx = 0

    def stream_chunk(self, data, startdate=None, enddate=None, aggregation=None):
        """
        Compute chunks for a dataset using startdate, enddate and aggregation defined by the constructor.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            enddate (str): the ending date for streaming the data (e.g. '2021-01-01') (None)
            aggregation (str): the streaming frequency in pandas style (1M, 7D etc.)

        Returns:
            A DataArrayResample object for the time axis
        """

        if not startdate:
            startdate = self.startdate
        if not enddate:
            enddate = self.enddate
        if not enddate:  # If it is still not defined use the end of the data
            enddate = data.time[-1].values
        if not aggregation:
            aggregation = self.aggregation
        if not aggregation:  # if it is still None set to step
            aggregation = "S"

        aggregation = frequency_string_to_pandas(aggregation)

        if startdate:
            tim = data.time.sel(time=slice(startdate, enddate))
        else:
            tim = data.time

        literal, numeric = extract_literal_and_numeric(aggregation)        

        if literal == 'S':
            #nsteps = np.maximum(int('0' + numeric), 1)  # this allows also "S" for "1S"
            timr = pd.Series(tim).groupby(by=(np.arange(0, len(tim)) // numeric))
        else:
            timr = tim.resample(time=aggregation)

        return timr

    def stream(self, data, startdate=None, enddate=None, aggregation=None,
               timechunks=None, reset=False):
        """
        Stream a chunk of a dataset using startdate, enddate and aggregation defined by the constructor.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            enddate (str): the ending date for streaming the data (e.g. '2021-01-01') (None)
            aggregation (str): the streaming frequency in pandas style (1M, 7D etc.)
            timechunks (DataArrayResample, optional): a precomputed chunked time axis
            reset (bool, optional): reset the streaming

        Returns:
            A xarray.Dataset containing the subset of the input data that has been streamed.
        """

        if reset:
            self.idx = 0

        if not timechunks:
            timechunks = self.stream_chunk(data, startdate=startdate, enddate=enddate, aggregation=aggregation)

        if self.idx >= len(timechunks):  # we have consumed all the data
            return None
        else:
            date1 = timechunks.first()[self.idx]
            date2 = timechunks.last()[self.idx]
            self.idx += 1
            return (data.sel(time=slice(date1, date2)))

    def generator(self, data, startdate=None, enddate=None, aggregation=None):
        """
        The generator method is designed to split data into smaller chunks of data for
        processing or analysis. It returns a generator object that yields the smaller chunks of data.
        The method can split the data based on either a specific time interval or by a specific number of samples.
        Aggregation (frequency/chunking) and starting and ending dates selected at class instantiation can
        be overwritten.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            enddate (str): the ending date for streaming the data (e.g. '2021-01-01') (None)
            aggregation (str): the streaming frequency in pandas style (1M, 7D etc.)
        Returns:
            A generator object that yields the smaller chunks of data.
        """

        timechunks = self.stream_chunk(data, startdate=startdate, enddate=enddate, aggregation=aggregation)

        self.idx = 0  # reset in case stream has been called before
        for idx in range(0, len(timechunks)):
            yield self.stream(data, startdate=startdate, enddate=enddate,
                              aggregation=aggregation, timechunks=timechunks)

    def reset(self):
        """
        Reset the state of the streaming process.
        This means that if the stream function is called again after calling reset_stream,
        it will start streaming the input data from the beginning.
        """
        self.idx = 0

"""Streaming Mixin for Reader"""

import pandas as pd
from aqua.logger import log_configure


class Streaming():
    """Streaming class to be used in Reader and elsewhere"""

    def __init__(self, stream_step=1, stream_unit='steps', stream_startdate=None, loglevel=None):
        """
        The Streaming constructor.

        Arguments:
            stream_step (int):      the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years', 'steps') (steps)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
            loglevel (string):      Level of logging according to logging module
                                    (default: log_level_default of loglevel())

        Returns:
            A `Streaming` class object.
        """

        # define the internal logger
        self.logger = log_configure(log_level=loglevel, log_name='Streaming')

        self.stream_index = 0
        self.stream_date = None
        self.stream_step = stream_step
        self.stream_startdate = stream_startdate
        self.stream_unit = stream_unit

    def stream(self, data, stream_step=None, stream_unit=None, stream_startdate=None):
        """
        The stream method is used to stream data by either a specific time interval
        or by a specific number of samples. If the unit parameter is specified, the
        data is streamed by the specified unit and stream_step (e.g. 1 month).
        If the unit parameter is not specified, the data is streamed by stream_step
        steps of the original time resolution of input data.

        If the stream function is called a second time, it will return the subsequent
        chunk of data in the sequence. The function keeps track of the state of the
        streaming process through the use of internal attributes. This allows the user
        to stream through the entire dataset in multiple calls to the function,
        retrieving consecutive chunks of data each time.

        If stream_startdate is not specified, the method will use the first date in the dataset.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            stream_step  (int):     the number of time steps to stream the data by (Default = 1)
            stream_unit (str):      the unit of time to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years', 'steps') (None)
            stream_startdate (str): the starting date for streaming the data
                                    (e.g. '2020-02-25') (None)
        Returns:
            A xarray.Dataset containing the subset of the input data that has been streamed.
        """

        if not stream_step:
            stream_step = self.stream_step

        if not stream_unit:
            stream_unit = self.stream_unit

        if not stream_startdate:
            stream_startdate = self.stream_startdate

        if not self.stream_date:
            if stream_startdate:
                self.stream_date = pd.to_datetime(stream_startdate)
            else:
                self.stream_date = pd.to_datetime(data.time[0].values)

        if self.stream_index == 0 and stream_startdate:
            self.stream_index = data.time.to_index().get_loc(pd.to_datetime(stream_startdate))

        if stream_unit in 'steps':
            start_index = self.stream_index
            stop_index = start_index + stream_step
            self.stream_index = stop_index
            return data.isel(time=slice(start_index, stop_index))
        else:
            start_date = self.stream_date
            stop_date = start_date + pd.DateOffset(**{stream_unit: stream_step})
            self.stream_date = stop_date
            return data.sel(time=slice(start_date, stop_date)).where(data.time != stop_date, drop=True)

    def reset_stream(self):
        """
        Reset the state of the streaming process.
        This means that if the stream function is called again after calling reset_stream,
        it will start streaming the input data from the beginning.
        """
        self.stream_index = 0
        self.stream_date = None

    def stream_generator(self, data, stream_step=1, stream_unit=None, stream_startdate=None):
        """
        The stream_generator method is designed to split data into smaller chunks of data for
        processing or analysis. It returns a generator object that yields the smaller chunks of data.
        The method can split the data based on either a specific time interval or by a specific number of samples.
        If the unit parameter is specified, the data is streamed by the specified unit and stream_step (e.g. 1 month).
        If the unit parameter is not specified, the data is streamed by stream_step steps of the original time
        resolution of input data.

        Arguments:
            data (xr.Dataset):      the input xarray.Dataset
            stream_step  (int):     the number of samples or time interval to stream the data by (Default = 1)
            stream_unit (str):      the unit of the time interval to stream the data by
                                    (e.g. 'hours', 'days', 'months', 'years') (None)
            stream_startdate (str): the starting date for streaming the data (e.g. '2020-02-25') (None)
        Returns:
            A generator object that yields the smaller chunks of data.
        """
        if stream_startdate:
            start_date = pd.to_datetime(stream_startdate)
        else:
            start_date = data.time[0].values
        if stream_unit:
            while start_date < data.time[-1].values:
                stop_date = pd.to_datetime(start_date) + pd.DateOffset(**{stream_unit: stream_step})
                yield data.sel(time=slice(start_date, stop_date)).where(data.time != stop_date, drop=True)
                start_date = stop_date
        if not stream_unit:
            start_index = data.time.to_index().get_loc(start_date)
            while start_index < len(data.time):
                stop_index = start_index + stream_step
                yield data.isel(time=slice(start_index, stop_index))
                start_index = stop_index

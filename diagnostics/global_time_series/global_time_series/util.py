"""Utility for the timeseries module"""
import xarray as xr
import pandas as pd
from aqua.util import extract_literal_and_numeric


def loop_seasonalcycle(data=None, startdate=None, enddate=None, freq='MS'):
    """
    Take the data, evaluate a seasonal cycle and repeat it over a required time period

    Args:
        cycle (xr.DataArray): The seasonal cycle
        startdate (str): The start date of the required time period
        enddate (str): The end date of the required time period
        freq (str): The frequency of the time period (default 'MS' for monthly)
    """
    if data is None:
        raise ValueError('Data not provided')
    if startdate is None or enddate is None:
        raise ValueError('Start date or end date not provided')

    if freq == 'MS' or freq == 'mon' or freq == 'monthly':
        cycle = data.groupby('time.month').mean('time')
    elif freq == 'YS' or freq == 'yearly' or freq == 'annual':
        cycle = data.mean('time')
    else:
        raise ValueError(f'Frequency {freq} not supported')

    time_range = pd.date_range(start=startdate, end=enddate, freq=freq)

    if freq == 'MS' or freq == 'mon' or freq == 'monthly':
        months_data = []
        for i in range(1, 13):
            months_data.append(cycle[i-1].values)

        loop_data = []
        for timestamp in time_range:
            loop_data.append(months_data[timestamp.month-1])
    elif freq == 'YS' or freq == 'yearly' or freq == 'annual':
        years_data = cycle.values

        _, numeric = extract_literal_and_numeric('YS')
        offset = pd.DateOffset(months=6*numeric)
        time_range = time_range + offset
        loop_data = []
        for timestamp in time_range:
            loop_data.append(years_data)

    data = xr.DataArray(data=loop_data, coords=dict(time=time_range), dims=['time'])
    return data

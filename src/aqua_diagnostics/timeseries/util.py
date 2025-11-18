"""Utility for the timeseries module"""
import xarray as xr
import pandas as pd
from aqua.logger import log_configure

xr.set_options(keep_attrs=True)

def loop_seasonalcycle(data: xr.DataArray, startdate: str, enddate: str,
                       freq: str, center_time: bool = False, loglevel='WARNING'):
    """
    Take the data, evaluate a seasonal cycle and repeat it over a required time period

    Args:
        data (xr.DataArray): The data to be looped
        startdate (str): The start date of the required time period
        enddate (str): The end date of the required time period
        freq (str): The frequency of the time period (only 'monthly' or 'annual')
        center_time (bool): Whether to center the time value at the center of the month or year
        loglevel (str): The logging level (default 'WARNING')
    """
    logger = log_configure(loglevel, 'loop_seasonalcycle')

    if data is None:
        raise ValueError('Data not provided')
    if startdate is None or enddate is None:
        raise ValueError('Start date or end date not provided')
    if freq is None:
        raise ValueError('Frequency not provided')

    if freq == 'monthly':
        cycle = data.groupby('time.month').mean('time')
    elif freq == 'annual':
        cycle = data.mean('time')
    else:
        raise ValueError(f'Frequency {freq} not supported')

    logger.debug(f'Start: {startdate}, End: {enddate}, Freq: {freq} Center Time: {center_time}')

    if center_time:
            startdate = center_timestamp(pd.Timestamp(startdate), freq)
            enddate = center_timestamp(pd.Timestamp(enddate), freq)

    if freq == 'monthly':
        time_range = pd.date_range(start=startdate, end=enddate, freq='MS')
    
        if len(time_range) == 0:
            base_slice = cycle.isel(month=0)
            empty = base_slice.expand_dims(time=1).isel(time=slice(0, 0))
            return empty.assign_coords(time=time_range)

        months_data = [cycle.sel(month=i) for i in range(1, 13)]
        # Repeat slices over requested time range and concatenate lazily
        loop_slices = [months_data[timestamp.month - 1] for timestamp in time_range]
        data = xr.concat(loop_slices, dim='time', coords='different', compat='equals')
        data = data.drop_vars('month', errors='ignore')

    elif freq == 'annual':
        time_range = pd.date_range(start=startdate, end=enddate, freq='YS')

        if len(time_range) == 0:
            base_slice = cycle
            empty = base_slice.expand_dims(time=1).isel(time=slice(0, 0))
            return empty.assign_coords(time=time_range)
    
        # Repeat the single-year mean lazily for each year
        loop_slices = [cycle] * len(time_range)
        data = xr.concat(loop_slices, dim='time', coords='different', compat='equals')

    # Assign the requested time coordinate
    data = data.assign_coords(time=time_range)

    return data

def center_timestamp(time: pd.Timestamp, freq: str):
    """
    Center the time value at the center of the month or year

    Args:
        time (str): The time value
        freq (str): The frequency of the time period (only 'monthly' or 'annual')

    Returns:
        pd.Timestamp: The centered time value
    
    Raises:
        ValueError: If the frequency is not supported
    """
    if freq == 'monthly':
        center_time = time + pd.DateOffset(days=15)
    elif freq == 'annual':
        center_time = time + pd.DateOffset(months=6)
    else:
        raise ValueError(f'Frequency {freq} not supported')
    
    return center_time

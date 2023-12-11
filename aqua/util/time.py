"""
Module including time utilities for AQUA
"""

import math
import re
import pandas as pd
import xarray as xr
from pandas.tseries.frequencies import to_offset
from aqua.logger import log_configure


def frequency_string_to_pandas(freq):
    """
    Convert a string from the AQUA convention to 
    the usual pandas frequency standard
    """

    trans = {
        'hourly': '1H',
        'daily': '1D',
        'weekly': '1W',
        'monthly': '1M',
        'yearly': '1Y',
        'hour': '1H',
        'day': '1D',
        'pentad': '5D',
        'week': '1W',
        'month': '1M',
        'year': '1Y',
        'hours': '1H',
        'days': '1D',
        'pentads': '5D',
        'weeks': '1W',
        'months': '1M',
        'years': '1Y',
    }

    new_freq = trans.get(freq, freq)

    return new_freq


def _xarray_timedelta_string(xdataset):
    """
    Given a Xarray Dataset, estimate the time frequency and convert
    it as a Pandas frequency string
    """

    # to check if this is necessary
    timedelta = pd.Timedelta(xdataset.time.diff('time').mean().values)

    hours = math.floor(timedelta.total_seconds() / 3600)
    days = math.floor(hours / 24)
    months = math.floor(days / 28)  # Minimum month has around 28 days
    years = math.floor(days / 365)  # Assuming an average year has around 365 days

    #print([hours, days, months, years])
    
    if years >= 1:
        return f"{years}Y"
    elif months >= 1:
        return f"{months}M"
    elif days >= 1:
        return f"{days}D"
    else :
        return f"{hours}H"
    
def _find_end_date(start_date, offset):
    """Given a date and an offset in the form of pandas frequency
    return the expected end date of that period"""

    start_date = pd.Timestamp(start_date)
    end_date = start_date + to_offset(offset)
    # this because to_offset does not go to next month/year
    if 'Y' in offset or 'M' in offset:
        end_date = end_date + pd.DateOffset(days=1)
    return end_date

def _generate_expected_time_series(start_date, frequency, time_period):
    """Given a start date, a pandas frequency and the data_frequency generate
    an expected time series"""
 
    end_date = _find_end_date(start_date, time_period)
    time_series = pd.date_range(start=start_date, end=end_date, freq=frequency, inclusive='left')

    return time_series


def check_chunk_completeness(xdataset, resample_frequency='1D', loglevel='WARNING'):
    """Support function for timmean().
    Verify that all the chunks available in a dataset are complete given a
    fixed resample_frequency.
    
    Args:
        xdataset: An xarray dataset
        resample_frequency: the frequency on which we are planning to resample, based on pandas frequency
        
    Raise:
        ValueError if the there no available chunks
        
    Returns:
        A Xarray DataArray binary, 1 for complete chunks and 0 for incomplete ones, to be used by timmean()
        """
    
    logger = log_configure(loglevel, 'timmean_chunk_completeness')
    
    # get frequency of the dataset. Expected to be regular!
    data_frequency = _xarray_timedelta_string(xdataset)

    # normalize periods
    normalized_dates = xdataset.time.to_index().to_period(resample_frequency).to_timestamp()

    # the pandas monthly frequency is forced to end of the month. use `MS` to go at the start
    if 'M' in resample_frequency:
        pandas_frequency = re.findall(r'\d+', resample_frequency)[0] + 'MS'
    elif 'Y' in resample_frequency:
        pandas_frequency = re.findall(r'\d+', resample_frequency)[0] + 'YS'
    else:
        pandas_frequency = resample_frequency
    chunks = pd.date_range(start=normalized_dates[0],
                           end=normalized_dates[-1],
                           freq=pandas_frequency)

    # if no chunks, no averages
    if len(chunks) == 0:
        raise ValueError(f'No chunks! Cannot compute average on {resample_frequency} period, not enough data')

    check_completeness = []
    # loop on the chunks
    for chunk in chunks:
        end_date = _find_end_date(chunk, resample_frequency)
        logger.debug('Processing chunk from %s to %s', chunk, end_date)
        expected_timeseries = _generate_expected_time_series(chunk, data_frequency,
                                                             resample_frequency)
        expected_len = len(expected_timeseries)
        # effective_len = len(xdataset.time.sel(time=slice(chunk, end_date)))
        effective_len = len(xdataset.time[(xdataset['time'] >= chunk) &
                                          (xdataset['time'] < end_date)])
        logger.debug('Expected chunk length: %s, Effective chunk length: %s', expected_len, effective_len)
        if expected_len == effective_len:
            check_completeness.append(True)
        else:
            logger.warning('Chunk %s->%s for has %s elements instead of expected %s, timmean() will exclude this',
                                expected_timeseries[0], expected_timeseries[-1], effective_len, expected_len)
            check_completeness.append(False)

        #except KeyError as exc:
        #    effective_len = len(xdataset.time.sel(time=slice(chunk, end_date)))
        #    logger.warning('Chunk %s->%s for has %s elements instead of expected %s, timmean() will exclude this',
        #                        chunk, end_date, effective_len, expected_len)
        #    check_completeness.append(False)
        #try: 
        #    effective_len = len(xdataset.time.sel(time=expected_timeseries))
        #    check_completeness.append(True)
        #except KeyError as exc:
        #    effective_len = len(xdataset.time.sel(time=slice(chunk, end_date)))
        #    logger.warning('Chunk %s->%s for has %s elements instead of expected %s, timmean() will exclude this',
        #                        chunk, end_date, effective_len, expected_len)
        #    check_completeness.append(False)
    
    # build the binary mask
    taxis = xdataset.time.resample(time=pandas_frequency).mean()
    if sum(check_completeness) == 0:
        logger.warning('Not enough data to compute any average on %s period, returning empty array', resample_frequency)
    
    #print(taxis)
    boolean_mask = xr.DataArray(check_completeness, dims=('time',), coords={'time': taxis.time})

    return boolean_mask

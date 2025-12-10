"""
Module including time utilities for AQUA
"""
import math
import numpy as np
import pandas as pd
import xarray as xr
from pandas.tseries.frequencies import to_offset
from aqua.core.util.sci_util import generate_quarter_months, TRIPLET_MONTHS
from aqua.core.util.string import get_quarter_anchor_month
from aqua.core.logger import log_configure

def frequency_string_to_pandas(freq):
    """
    Convert a string from the AQUA convention to
    the usual pandas frequency standard

    Args:
        freq (str): The frequency string to convert.

    Returns:
        str: The converted frequency string, pandas compliant.
    """
    logger = log_configure('WARNING', 'frequency_string_to_pandas')

    trans = {
        'hourly': 'h',
        'daily': 'D',
        'weekly': 'W',
        'seasonal': 'QS-DEC',
        'monthly': 'MS', #this implies start of the month
        'annual': 'YS', #this implies start of the year
        'yearly': 'YS', #this implies start of the year
        'hour': 'h',
        'day': 'D',
        'pentad': '5d',
        'week': 'W',
        'month': 'MS',
        'year': 'YS',
        'hours': 'h',
        'days': 'D',
        'pentads': '5D',
        'weeks': 'W',
        'months': 'MS',
        'years': 'YS',
    }

    new_freq = trans.get(freq, freq)

    if freq in ['M', 'ME', 'Y', 'YE']:
        logger.warning('You are using a pandas frequency pointing at the end of a period, this can behave unexpectedly if you have subdaily data')

    return new_freq


def xarray_to_pandas_freq(xdataset: xr.Dataset | xr.DataArray):
    """
    Given a Xarray Dataset, estimate the time frequency and convert
    it as a Pandas frequency string

    Args:
        xdataset (xr.Dataset | xr.DataArray): The input xarray object.

    Returns:
        str: The inferred time frequency as a string, pandas compliant.
    """
    # to check if this is necessary
    timedelta = pd.Timedelta(xdataset.time.diff('time').mean().values)

    hours = math.floor(timedelta.total_seconds() / 3600)
    days = math.floor(hours / 24)
    months = math.floor(days / 28)  # Minimum month has around 28 days
    years = math.floor(days / 365)  # Assuming an average year has around 365 days

    # print([hours, days, months, years])

    if years >= 1:
        return f"{years}Y"
    elif months >= 1:
        return f"{months}MS"
    elif days >= 1:
        return f"{days}D"
    else:
        return f"{hours}h"


def pandas_freq_to_string(freq: str) -> str:
    """
    Convert a pandas frequency string to a more human-readable format.
    It also supports already human-readable formats.

    Args:
        freq (str): The pandas frequency string.

    Returns:
        str: The human-readable format of the frequency.
    """
    trans = {
        # Hourly
        'H': 'hourly',
        'h': 'hourly',
        '1h': 'hourly',
        'hourly': 'hourly',
        # Daily
        'D': 'daily',
        'd': 'daily',
        '1D': 'daily',
        'daily': 'daily',
        # Weekly
        'W': 'weekly',
        'weekly': 'weekly',
        # Seasonal
        'QS-DEC': 'seasonal',
        'Q': 'seasonal',
        # Monthly
        '1MS': 'monthly',
        'MS': 'monthly',
        'M': 'monthly',
        'ME': 'monthly',
        'mon': 'monthly',
        'monthly': 'monthly',
        # Annual
        '1Y': 'annual',
        'YS': 'annual',
        'Y': 'annual',
        'YE': 'annual',
        'yearly': 'annual',
        'years': 'annual',
        'annual': 'annual'
    }

    return trans.get(freq, freq)


def _find_end_date(start_date, offset):
    """Given a date and an offset in the form of pandas frequency
    return the expected end date of that period"""

    start_date = pd.Timestamp(start_date)
    end_date = start_date + to_offset(offset)
    # this because to_offset does not go to next month/year
    #if 'Y' in offset or 'M' in offset:
    #    end_date = end_date + pd.DateOffset(days=1)
    return end_date


def _generate_expected_time_series(start_date, frequency, time_period):
    """
    Given a start date, a pandas frequency and the data_frequency generate
    an expected time series
    """

    end_date = _find_end_date(start_date, time_period)
    time_series = pd.date_range(start=start_date, end=end_date, freq=frequency, inclusive='left')

    return time_series


def chunk_dataset_times(xdataset, resample_frequency, loglevel):
    """
    Common setup for chunk completeness checking.
    
    Returns:
        tuple: (data_frequency, chunks)
    """
    logger = log_configure(loglevel, 'timmean_chunking')

    # get frequency of the dataset. Expected to be regular!
    data_frequency = xarray_to_pandas_freq(xdataset)
    logger.debug('Data frequency detected as: %s', data_frequency)

    # convert offset
    pandas_period = to_offset(resample_frequency)

    normalized_dates = xdataset.time.to_index().to_period(pandas_period).to_timestamp()
    chunks = pd.date_range(start=normalized_dates[0],
                           end=normalized_dates[-1],
                           freq=resample_frequency)
    
    logger.info('%s chunks from %s to %s at %s frequency to be analysed', 
                len(chunks), chunks[0], 
                chunks[-1], resample_frequency)
    
    # if no chunks, no averages
    if len(chunks) == 0:
        raise ValueError(f'No chunks! Cannot compute average on {resample_frequency} period, not enough data')

    return data_frequency, chunks


def check_chunk_completeness(xdataset, resample_frequency='1D', loglevel='WARNING'):
    """
    Support function for timmean().
    Verify that all the chunks available in a dataset are complete given a
    fixed resample_frequency.
    Args:
        xdataset: The original dataset before averaging
        resample_frequency: the frequency on which we are planning to resample, based on pandas frequency
    Raise:
        ValueError if the there no available chunks
    Returns:
        A Xarray DataArray binary, 1 for complete chunks and 0 for incomplete ones, to be used by timmean()
    """

    data_frequency, chunks = chunk_dataset_times(xdataset, resample_frequency, loglevel)

    logger = log_configure(loglevel, 'timmean_chunk_completeness')

    check_completeness = []

    for chunk in chunks:
        end_date = _find_end_date(chunk, resample_frequency)
        logger.debug('Processing chunk from %s to %s', chunk, end_date)
        expected_timeseries = _generate_expected_time_series(chunk, data_frequency,
                                                             resample_frequency)
        expected_len = len(expected_timeseries)

        effective_len = len(xdataset.time[(xdataset['time'] >= chunk) &
                                          (xdataset['time'] < end_date)])
        logger.debug('Expected chunk length: %s, Effective chunk length: %s', expected_len, effective_len)
        if expected_len == effective_len:
            check_completeness.append(True)
        else:
            logger.warning('Chunk %s->%s for has %s elements instead of expected %s, timmean() will exclude this',
                           expected_timeseries[0], expected_timeseries[-1], effective_len, expected_len)
            check_completeness.append(False)

    # build the binary mask
    taxis = xdataset.time.resample(time=resample_frequency).mean()
    if sum(check_completeness) == 0:
        logger.warning('Not enough data to compute any average on %s period, returning empty array', resample_frequency)

    # Create a dict mapping chunk dates to completeness flag
    completeness_dict = {pd.Timestamp(chunk): is_complete for chunk, is_complete in zip(chunks, check_completeness)}

    # Align with the actual resampled time axis
    aligned_completeness = [completeness_dict.get(pd.Timestamp(t), False) for t in taxis.time.values]

    boolean_mask = xr.DataArray(aligned_completeness, dims=('time',), coords={'time': taxis.time})

    return boolean_mask


def check_seasonal_chunk_completeness(xdataset, resample_frequency='QS-DEC', loglevel='WARNING'):
    """
    Support function for timmean() to check seasonal chunk completeness.
    Verify that all seasonal (quarterly) chunks have complete months.
    
    For seasonal data (QS-DEC), this checks if each quarter has all 3 of its
    constituent months. Uses the same season definitions as select_season().
    
    Args:
        xdataset: The original dataset before averaging
        resample_frequency: the frequency on which we are planning to resample, 
                          expected to be 'QS-DEC' (or similar quarterly frequency)
        loglevel: logging level
    
    Raise:
        ValueError if there are no available chunks
    
    Returns:
        A Xarray DataArray binary, 1 for complete quarters and 0 for incomplete ones
    """
    data_frequency, chunks = chunk_dataset_times(xdataset, resample_frequency, loglevel)

    anchor_month = get_quarter_anchor_month(resample_frequency)

    logger = log_configure(loglevel, 'timmean_seasonal_completeness')

    # Generate quarter months: e.g. {'DEC': {'Q1': [12,1,2], 'Q2': [3,4,5], ...}}
    quarters_full = generate_quarter_months(anchor_month)
    quarter_months = quarters_full[anchor_month] # e.g. {'Q1': [12, 1, 2], 'Q2': [3, 4, 5], ...}

    # Build season_months: map from start_month 
    # e.g., {12: {12, 1, 2}, 3: {3, 4, 5}, 6: {6, 7, 8}, 9: {9, 10, 11}}
    season_months = {}
    for quarter_key in ['Q1', 'Q2', 'Q3', 'Q4']:
        months_list = quarter_months[quarter_key] # e.g. [12, 1, 2]
        start_month = months_list[0]
        season_months[start_month] = set(months_list)

    if 'D' in data_frequency or 'h' in data_frequency:
        logger.info('Data is sub-monthly (%s), first checking monthly completeness...', data_frequency)
        monthly_mask = check_chunk_completeness(xdataset, 
                                                resample_frequency='MS',
                                                loglevel=loglevel)
        # Get only complete months, use .resample().mean() to get time axis
        complete_month_times = xdataset.time.resample(time='MS').mean().time
        complete_month_times = complete_month_times.where(monthly_mask, drop=True)

        # Store complete month-year combinations as timestamps
        complete_month_timestamps = set(complete_month_times.to_index())
        logger.debug('Complete months available: %s', sorted(complete_month_timestamps))
    else:
        complete_month_timestamps = set(xdataset.time.to_index())
        logger.debug('Retrieved data frequency is monthly or coarser, using all months')

    check_completeness = []
    for chunk in chunks:
        end_date = _find_end_date(chunk, resample_frequency)
        logger.debug('Processing seasonal chunk from %s to %s', chunk, end_date)

        # Determine which season this is based on the start month
        start_month = chunk.month
        expected_months = season_months.get(start_month, set())

        # Get actual months present in this quarter period
        quarter_data = xdataset.time[(xdataset['time'] >= chunk) & 
                                     (xdataset['time'] < end_date)]

        if len(quarter_data) == 0:
            actual_months = set()
        else:
            # Extract which months we actually have
            actual_months = set(quarter_data.to_index().month)

        # Check if all expected months are present and complete
        has_all_months = expected_months.issubset(actual_months)

        # Additionally check that these months are complete
        if 'D' in data_frequency or 'h' in data_frequency:
            # Check completeness for the specific months within this quarter period
            # Generate the expected month timestamps for this quarter
            quarter_start = pd.Timestamp(chunk)
            quarter_end = pd.Timestamp(end_date)
            # Get all month starts within this quarter e.g. [2024-12-01, 2025-01-01, 2025-02-01]
            quarter_month_starts = pd.date_range(start=quarter_start, end=quarter_end, freq='MS', inclusive='left')
            # Check if all these specific month-year combinations are complete
            months_are_complete = quarter_month_starts.isin(complete_month_timestamps).all()
            is_complete = has_all_months and months_are_complete
        else:
            # For monthly data, just check presence
            is_complete = has_all_months

        if is_complete:
            check_completeness.append(True)
        else:
            season_name = mon_to_quarter_season_name(start_month)
            logger.warning(f"Seasonal chunk {chunk.strftime('%Y-%m')} ({season_name}) incomplete: expected months "
                           f"{sorted(expected_months)}, found {sorted(actual_months)}, timmean() will exclude this")
            check_completeness.append(False)

    if sum(check_completeness) == 0:
        logger.warning(f'Not enough data to compute any complete seasonal average on {resample_frequency} period, returning empty array')

    taxis = xdataset.time.resample(time=resample_frequency).mean()

    # Create a dict mapping chunk dates to completeness flag
    completeness_dict = {pd.Timestamp(chunk): is_complete for chunk, is_complete in zip(chunks, check_completeness)}

    # Align with the actual resampled time axis
    aligned_completeness = [completeness_dict.get(pd.Timestamp(t), False) for t in taxis.time.values]

    boolean_mask = xr.DataArray(aligned_completeness, dims=('time',), coords={'time': taxis.time})

    return boolean_mask


def time_to_string(time=None, format='%Y-%m-%d'):
    """Convert a time object to a string in the format YYYY-MM-DD

    Args:
        time: a time object, either a string, a datetime64 object or a pandas timestamp
        format: the format of the output string (YYYY-MM-DD by default, YYYYMMDD tested)

    Returns:
        A string in the format defined by the format argument

    Raises:
        ValueError if time is None or if time is not a supported type
    """
    if time is None:
        raise ValueError('time_to_string() requires a time argument')

    # Timestap for safer string handling
    if isinstance(time, str):
        time = pd.Timestamp(time)
    # Convert supported types into pandas.Timestamp
    if isinstance(time, (pd.Timestamp, np.datetime64)):
        ts = pd.to_datetime(time)
    else:
        raise ValueError('time_to_string() requires a time argument of type str, pd.Timestamp or np.datetime64')
    return ts.strftime(format)


def int_month_name(month, abbreviated=False):
    """
    Return month name from integer (1-12) if xarray functions cannot be used
    
    Args:
        month (int): The month as an integer (1-12).
        abbreviated (bool): Whether to return the abbreviated month name (default is False).

    Returns:
        str: The name of the month.
    """
    name = ["January", "February", "March", "April", "May", "June",
            "July", "August", "September", "October", "November", "December"][month - 1]
    return name[:3] if abbreviated else name


def mon_to_quarter_season_name(month):
    """
    Convert a month number (starting month of a quarter) to season abbreviation name.
    For QS-DEC, the quarter start months are 12, 3, 6, 9. Map them to their season based on TRIPLET_MONTHS
    
    Args:
        month (int): Month number (1-12), expected to be a quarter start month
    
    Returns:
        str: Season abbreviation (e.g., 'DJF', 'MAM', 'JJA', 'SON')
    """
    for season_name, months in TRIPLET_MONTHS.items():
        if months[0] == month:
            return season_name

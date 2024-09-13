"""Utilities for calendar and timestep calculations"""

import pandas as pd
from datetime import datetime
import os


def date2str(dateobj):
    """
    Converts a date object to a date, time string representation

    Args:
        dateobj (datetime): datetime object

    Returns:
        date, time (str): Date and time as a strings
    """

    return dateobj.strftime('%Y%m%d'), dateobj.strftime('%H%M')

def date2yyyymm(dateobj):
    """
    Converts a date object to year, month string representation

    Args:
        dateobj (datetime): datetime object

    Returns:
        year, month (str): Date as year and month
    """

    return dateobj.strftime('%Y'), dateobj.strftime('%m')


def add_offset(data_start_date, startdate, offset, timestep):
    """
    Sets initial date based on an offset in steps (special for DE 6h case)

    Args:
        data_start_date (datetime.datetime): Data start date
        startdate (datetime.datetime): User-defined starting date
        offset (int): Offset to add to data start date
        timestep (int): Timestep to use (only H and D implemented)

    Returns:
        New starting date (str)
    """

    if int(offset) != 0:
        if timestep.upper() in ["H", "D"]:
            base_date = pd.Timestamp(str(data_start_date)) + pd.Timedelta(int(offset), unit=timestep)
        else:
            raise ValueError("Timestep not supported")

        startdate_obj = pd.Timestamp(str(startdate))
        if startdate_obj > base_date:
            base_date = startdate_obj

        return base_date.strftime('%Y%m%dT%H%M')
    else:
        return startdate


def check_dates(startdate, start_date, enddate, end_date):
    """
    Check if starting and ending dates are within given range

    Args:
        startdate (datetime.datetime): Starting date
        start_date (datetime.datetime): Data start date
        enddate (datetime.datetime): Ending date
        end_date (datetime.datetime): Data end date

    Raises:
        ValueError: If the given date is not within the data range
    """

    startdate_fmt = "%Y%m%dT%H%M" if 'T' in str(startdate) else "%Y%m%d"
    start_date_fmt = "%Y%m%dT%H%M" if 'T' in str(start_date) else "%Y%m%d"
    enddate_fmt = "%Y%m%dT%H%M" if 'T' in str(enddate) else "%Y%m%d"
    end_date_fmt = "%Y%m%dT%H%M" if 'T' in str(end_date) else "%Y%m%d"

    if datetime.strptime(str(startdate), startdate_fmt) < datetime.strptime(str(start_date), start_date_fmt):
        raise ValueError(f"Starting date {str(startdate)} is earlier than the data start at {str(start_date)}.")

    if datetime.strptime(str(enddate), enddate_fmt) > datetime.strptime(str(end_date), end_date_fmt):
        raise ValueError(f"End date {str(enddate)} is later than the data end at {str(end_date)}.")

    if datetime.strptime(str(startdate), startdate_fmt) > datetime.strptime(str(enddate), enddate_fmt):
        raise ValueError(f"Start date {str(startdate)} is later than the end date at {str(enddate)}.")

    if datetime.strptime(str(start_date), start_date_fmt) > datetime.strptime(str(end_date), end_date_fmt):
        raise ValueError(f"Data start date {str(start_date)} is later than the data end at {str(end_date)}.")


def shift_time_dataset(data):
    """
    Shift time of a dataset back one month
    Args:
        data (xarray.DataSet): The dataset to shift
    Returns:
        A revised xarray.DataSet
    """

    newtime = [d + pd.DateOffset(months=-1) for d in data.time.data]
    return data.assign_coords(time=newtime)


def split_date(datestr, timedefault="0000"):
    """
    Splits a date in YYYYMMDD:HHMM format into date and time strings
    Args:
        datestr (str): The date string to split
        timedefault (str): Default for time if not provided
    Returns:
        date and time as str
    """

    dd = str(datestr).split('T')
    timestr = dd[1] if "T" in str(datestr) else timedefault
    return dd[0], timestr


def make_timeaxis(data_startdate, startdate, enddate, timestep=None,
                  savefreq=None, chunkfreq=None, shiftmonth=False, skiplast=False):
    """
    Compute timeaxis and chunk start and end dates and indices.

    Args:
        data_startdate (datetime.datetime): Starting date of the dataset
        startdate (datetime.datetime): Starting date of the time axis
        enddate (datetime.datetime): Ending date of the time axis
        offset (int): An initial offset for steps (to be used e.g. for 6H data saved starting from step=6)
        timestep (str): Timestep. Can be one of h, D, M
        savefreq (str): Frequency at which the data are saved. Can be one of h, 6h, D, M
        chunkfreq (str): Frequency at which the data are to be chunked. Can be one of D, M, Y
        shiftmonth (bool): If True, fixes data accumulated at the end of the month. Default is False.
        skiplast (bool): If True, skips the last date. Default is False.

    Returns:
        A tuple containing:
            - timeaxis (pd.Series): The time axis
            - chunkstart_idx (int): The starting index of each chunk
            - chunkstart (pd.Timestamp): The start date of the first chunk
            - chunkend_idx (int): The last index of each chunk
            - chunkend (pd.Timestamp): The end date of each chunk
            - chunksize (int): The number of data points in each chunk
    """

    # these are equivalent, unless specified different
    if savefreq is None:
        savefreq = timestep
    if timestep is None:
        timestep = savefreq
    if chunkfreq is None:
        chunkfreq = timestep

    if shiftmonth and savefreq != "M":
        raise ValueError("shiftmonth option requested but data are not saved at monthly frequency!")

    # compute offset
    offset = len(pd.date_range(str(data_startdate), str(startdate), freq=timestep)) - 1

    sdate = pd.Timestamp(str(startdate))
    edate = pd.Timestamp(str(enddate))

    if shiftmonth:  # We will need one month more
        edate = edate + pd.offsets.MonthBegin()

    if skiplast:
        edate = edate - pd.Timedelta(1, unit=timestep)

    dates = pd.date_range(sdate, edate, freq=timestep)
    idx = range(len(dates))
    ts = pd.Series(idx, index=dates)

    if timestep != savefreq:
        # the data are saved at a frequency different from the original timestep (eg. monthly)
        # do a preliminary resampling
        if shiftmonth:
            idx = ts.resample(savefreq).min().values
            ts = pd.Series(idx[1:], index=dates[idx[0:-1]])  # index of the next month but date of previous one
            idx = idx[0:-1]
        else:
            idx = ts.resample(savefreq).min().values
            ts = pd.Series(idx, index=dates[idx])

    tsr = ts.resample(chunkfreq)
    sidx = tsr.min().values
    eidx = tsr.max().values
    chunksize = tsr.count().values

    if shiftmonth:  # special case, data is accumulated at end of month
        sdate = dates[sidx] - pd.offsets.MonthBegin()
        edate = dates[eidx] - pd.offsets.MonthBegin()
    else:
        sdate = dates[sidx]
        edate = dates[eidx]

    return {
        'timeaxis': dates[idx],
        'start_idx': sidx + offset,
        'start_date': sdate,
        'end_idx': eidx + offset,
        'end_date': edate,
        'size': chunksize
    }


def todatetime(datestr):
    """
    Converts a date string to a datetime object

    Args:
        datestr (str): Date string

    Returns:
        datetime object
    """

    return pd.Timestamp(str(datestr))


def read_bridge_end_date(obj):
    """
    Reads the bridge end date from a file or string
    """
    
    if obj and obj != "complete" and os.path.isfile(obj):
        with open(obj, 'r') as file:
            date = file.read()
        date = pd.Timestamp(date.strip())
        date += pd.DateOffset(days=1)
        return (date.strftime('%Y%m%d'))
    else:
        return obj
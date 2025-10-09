"""Time utilities for AQUA diagnostics"""
import pandas as pd


def start_end_dates(startdate=None, enddate=None,
                    start_std=None, end_std=None):
    """
    Evaluate start and end dates for the reference data retrieve,
    in the case both are provided, to minimize the Reader calls.
    They should be of the form 'YYYY-MM-DD' or 'YYYYMMDD'.
    The function will translate them to the form 'YYYY-MM-DD' and
    then use pandas Timestamp to evaluate the minimum and maximum
    dates.

    Args:
        startdate (str): start date for the data retrieve
        enddate (str): end date for the data retrieve
        start_std (str): start date for the standard deviation data retrieve
        end_std (str): end date for the standard deviation data retrieve

    Returns:
        tuple (str, str): start and end dates for the data retrieve
    """
    # Convert to pandas Timestamp
    startdate = pd.Timestamp(startdate) if startdate else None
    enddate = pd.Timestamp(enddate) if enddate else None
    start_std = pd.Timestamp(start_std) if start_std else None
    end_std = pd.Timestamp(end_std) if end_std else None

    start_retrieve = min (filter(None, [startdate, start_std])) if startdate else None
    end_retrieve = max (filter(None, [enddate, end_std])) if enddate else None

    return start_retrieve, end_retrieve

def round_startdate(startdate):
    """
    Round the start date to the beginning of the month
    
    Args:
        startdate (str or pandas.Timestamp): start date for the data retrieve

    Returns:
        pandas.Timestamp: start date rounded to the beginning of the month
    """
    startdate = pd.Timestamp(startdate)
    startdate = startdate.replace(day=1)
    startdate = startdate.replace(hour=0, minute=0, second=0)

    return startdate

def round_enddate(enddate):
    """
    Round the end date to the end of the month
    
    Args:
        enddate (str or pandas.Timestamp): end date for the data retrieve

    Returns:
        pandas.Timestamp: end date rounded to the end of the month
    """
    enddate = pd.Timestamp(enddate)
    endday_dict = {1: 31, 2: 29 if enddate.year % 4 == 0 else 28,
                   3: 31, 4: 30, 5: 31, 6: 30,
                   7: 31, 8: 31, 9: 30, 10: 31,
                   11: 30, 12: 31}

    enddate = enddate.replace(day=endday_dict[enddate.month])
    enddate = enddate.replace(hour=23, minute=59, second=59)
    return enddate
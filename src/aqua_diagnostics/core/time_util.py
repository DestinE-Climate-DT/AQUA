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

    start_retrieve = min(filter(None, [startdate, start_std])) if startdate else None
    end_retrieve = max(filter(None, [enddate, end_std])) if enddate else None

    return start_retrieve, end_retrieve


def round_startdate(startdate, freq='monthly'):
    """
    Round the start date to the start of the month or year.
    
    Args:
        startdate (pd.Timestamp): start date
        freq (str): frequency ('monthly' or 'annual'). Default is 'monthly'.
    
    Returns:
        pd.Timestamp: rounded start date
    """
    if freq == 'annual':
        return pd.Timestamp(year=startdate.year, month=1, day=1, 
                           hour=0, minute=0, second=0)
    elif freq == 'monthly':
        return pd.Timestamp(year=startdate.year, month=startdate.month, day=1,
                           hour=0, minute=0, second=0)
    else:
        raise ValueError(f"Unsupported frequency '{freq}'. Only 'monthly' and 'annual' are supported.")


def round_enddate(enddate, freq='monthly'):
    """
    Round the end date to the end of the month or year.
    
    Args:
        enddate (pd.Timestamp): end date
        freq (str): frequency ('monthly' or 'annual'). Default is 'monthly'.
    
    Returns:
        pd.Timestamp: rounded end date
    """
    if freq == 'annual':
        return pd.Timestamp(year=enddate.year, month=12, day=31,
                           hour=23, minute=59, second=59)
    elif freq == 'monthly':
        return pd.Timestamp(year=enddate.year, month=enddate.month, day=1,
                           hour=0, minute=0, second=0) + pd.DateOffset(months=1) - pd.Timedelta(seconds=1)
    else:
        raise ValueError(f"Unsupported frequency '{freq}'. Only 'monthly' and 'annual' are supported.")
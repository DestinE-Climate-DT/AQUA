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
    for time in [startdate, enddate, start_std, end_std]:
        if time is not None:
            try:
                pd.Timestamp(time)
            except ValueError:
                raise ValueError(f"Invalid date format: {time}. Use 'YYYY-MM-DD' or 'YYYYMMDD'.")

    start_retrieve = min(filter(None, [startdate, start_std])) if any([startdate, start_std]) else None
    end_retrieve = max(filter(None, [enddate, end_std])) if any([enddate, end_std]) else None

    # Reconverting to string "YYYY-MM-DD"
    if start_retrieve is not None:
        start_retrieve = pd.Timestamp(start_retrieve).strftime('%Y-%m-%d')
    if end_retrieve is not None:
        end_retrieve = pd.Timestamp(end_retrieve).strftime('%Y-%m-%d')

    return start_retrieve, end_retrieve

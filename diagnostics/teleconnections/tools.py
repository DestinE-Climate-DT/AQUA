import numpy as np
import pandas as pd
import xarray as xr

'''
def season_mean(ds, calendar='standard'):
    # DataArray with number of days in each month
    month_length = ds.time.dt.days_in_month

    # Calculate the weights by grouping by 'time.season'
    weights = month_length.groupby("time.season") / month_length.groupby("time.season").sum()

    # Test that the sum of the weights for each season is 1.0
    np.testing.assert_allclose(weights.groupby("time.season").sum().values, np.ones(4))

    # Calculate the weighted average
    return (ds * weights).groupby("time.season").sum(dim="time")
'''

def lon_check (lon):
    """ 
    Convert longitude to [0,360] range.

    Parameters
    ----------
    lon : float

    Returns
    -------
    float
    """
    if (lon<0):
        print('ok')
        lon = (lon+180) % 360 
        print(lon)
    return lon
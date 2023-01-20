#import numpy as np
#import pandas as pd
#import xarray as xr

def lon_180_to_360 (lon):
    """ 
    Convert longitude [-180,180] to [0,360] range.

    Parameters
    ----------
    lon : float

    Returns
    -------
    float
    """
    if (lon<0):
        lon = 360 + lon
    return lon

def lon_360_to_180 (lon):
    """ 
    Convert longitude [0,360] to [-180,180] range.

    Parameters
    ----------
    lon : float

    Returns
    -------
    float
    """
    if (lon>180):
        lon = - 360 + lon
    return lon
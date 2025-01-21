'''
This module contains scientific tools for the teleconnections diagnostic.
- area selection functions, to deal with custom areas
- conversion functions, to deal with conversion between different
  physical units.
- weighted area mean function, to deal with weighted area mean
'''
import numpy as np
import xarray as xr
from aqua.logger import log_configure
from aqua.util import area_selection


def lon_180_to_360(lon: float):
    """
    Convert longitude [-180,180] to [0,360] range.

    Args:
        lon (float): longitude coordinate

    Returns:
        lon (float): converted longitude
    """
    if lon < 0:
        lon = 360 + lon
    return lon


def lon_360_to_180(lon: float):
    """
    Convert longitude [0,360] to [-180,180] range.

    Args:
        lon (float): longitude coordinate

    Returns:
        lon (float): converted longitude
    """
    if lon > 180:
        lon = - 360 + lon
    return lon


def wgt_area_mean(indat, latN: float, latS: float,
                  lonW: float, lonE: float, box_brd=True,
                  loglevel='WARNING'):
    """
    Evaluate the weighted mean of a quantity on a custom surface.

    Args:
        indat (xarray.DataArray): input data to be averaged
        latN (float):             North latitude
        latS (float):             South latitude
        lonW (float):             West longitude
        lonE (float):             Est longitude
        box_brd (bool,opt):       choose if coordinates are comprised or not.
                                  Default is True
        loglevel (str, opt):      log level, default is WARNING

    Returns:
        (xarray.DataArray): average of input data on a custom surface
    """
    # 0. -- Logging --
    logger = log_configure(loglevel, 'weighted area mean')

    # 1. -- Extract coordinates from indat --
    lat = indat.lat

    # 2. -- Select area --
    indat = area_selection(data=indat, lat=[latS, latN],
                           lon=[lonW, lonE], box_brd=box_brd,
                           loglevel=loglevel)

    # 3. -- Weighted area mean --
    logger.debug('Computing weighted area mean')

    # TODO: check if this is still needed
    # # Rechunk to avoid memory issues
    # indat = indat.chunk({'time': -1, 'lat': 1, 'lon': 1})
    wgt = np.cos(np.deg2rad(lat))
    odat = indat.weighted(wgt).mean(("lon", "lat"), skipna=True)
    # HACK added with ICON, to avoid NaNs in the output
    odat.dropna(dim='time', how='all')

    return odat


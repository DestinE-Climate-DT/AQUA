'''
This module contains scientific tools for the teleconnections diagnostic.
- area selection functions, to deal with custom areas
- conversion functions, to deal with conversion between different
  physical units.
- weighted area mean function, to deal with weighted area mean
'''
import numpy as np
from aqua.logger import log_configure


def area_selection(indat, lat=None, lon=None, box_brd=True,
                   loglevel='WARNING'):
    """
        Extract a custom area from a DataArray.

        Args:
            indat (xarray.DataArray): input data to be selected
            lat (list, opt):          latitude coordinates
            lon (list, opt):          longitude coordinates
            box_brd (bool,opt):       choose if coordinates are
                                      comprised or not.
                                      Default is True
            loglevel (str, opt):      log level, default is WARNING

        Returns:
            (xarray.DataArray):  data on a custom surface

        Raises:
            ValueError: if lat and lon are both None
            ValueError: if lat or lon are not in ascending order
            AttributeError: if lat or lon are not found in input data
    """
    # 0. -- Logging --
    logger = log_configure(loglevel, 'area selection')
    logger.debug("Selecting area: lat = %s, lon = %s", lat, lon)

    # 1. -- Extract coordinates from indat --
    if lat is None and lon is None:
        raise ValueError('lat and lon cannot be both None')
    if lat:
        if lat[0] > lat[1]:
            raise ValueError('lat must be specified in ascending order')
        else:
            try:
                lat_coord = indat.lat
            except AttributeError:
                raise AttributeError('lat not found in input data')
    if lon:
        if lon[0] > lon[1]:
            raise ValueError('lon must be specified in ascending order')
        else:
            try:
                lon_coord = indat.lon
            except AttributeError:
                raise AttributeError('lon not found in input data')

    # 2. -- Select area --
    if box_brd:
        logger.debug('Selecting area with box boundaries')
        if lat:
            iplat = lat_coord.where((lat_coord >= lat[0]) &
                                    (lat_coord <= lat[1]), drop=True)
        if lon:
            iplon = lon_coord.where((lon_coord >= lon[0]) &
                                    (lon_coord <= lon[1]), drop=True)
    else:
        logger.debug('Selecting area without box boundaries')
        if lat:
            iplat = lat_coord.where((lat_coord > lat[0]) &
                                    (lat_coord < lat[1]), drop=True)
        if lon:
            iplon = lon_coord.where((lon_coord > lon[0]) &
                                    (lon_coord < lon[1]), drop=True)

    # 3. -- Area selection --
    odat = indat
    if lat:
        logger.debug('Selecting latitudes')
        odat = odat.sel(lat=iplat)
    if lon:
        logger.debug('Selecting longitudes')
        odat = odat.sel(lon=iplon)

    logger.debug('Area selected')
    return odat


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
    indat = area_selection(indat, lat=[latS, latN],
                           lon=[lonW, lonE], box_brd=box_brd,
                           loglevel=loglevel)

    # 3. -- Weighted area mean --
    logger.debug('Computing weighted area mean')

    # Rechunk to avoid memory issues
    indat = indat.chunk({'time': -1, 'lat': 1, 'lon': 1})

    wgt = np.cos(np.deg2rad(lat))
    odat = indat.weighted(wgt).mean(("lon", "lat"), skipna=True)
    # HACK added with ICON, to avoid NaNs in the output
    odat.dropna(dim='time', how='all')

    return odat

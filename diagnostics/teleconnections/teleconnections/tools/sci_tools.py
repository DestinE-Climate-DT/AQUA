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
    logger = log_configure(loglevel, 'area selection')
    logger.debug("Selecting area: lat = %s, lon = %s", lat, lon)

    # Extract coordinates from indat
    if lat is None and lon is None:
        raise ValueError('lat and lon cannot be both None')
    if lat:
        if lat[0] > lat[1]:
            raise ValueError('lat must be specified in ascending order')
        try:
            lat_coord = indat.lat
        except AttributeError as err:
            raise AttributeError('lat not found in input data') from err
    if lon:
        if lon[0] > lon[1]:
            raise ValueError('lon must be specified in ascending order')
        try:
            lon_coord = indat.lon
        except AttributeError as err:
            raise AttributeError('lon not found in input data') from err

    # Select area
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

    # Area selection
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


def select_season(xr_data: xr.DataArray or xr.Dataset, season: str):
    """
    Select a season from a xarray.DataArray or xarray.Dataset.
    Available seasons are:
    - DJF: December-January-February
    - JFM: January-February-March
    - FMA: February-March-April
    - MAM: March-April-May
    - AMJ: April-May-June
    - MJJ: May-June-July
    - JJA: June-July-August
    - JAS: July-August-September
    - ASO: August-September-October
    - SON: September-October-November
    - OND: October-November-December
    - NDJ: November-December-January

    Args:
        xr_data (xarray.DataArray or xarray.Dataset): input data
        season (str):                                 season to be selected

    Returns:
        (xarray.DataArray or xarray.Dataset): selected season
    """
    triplet_months = {
        'DJF': [12, 1, 2],
        'JFM': [1, 2, 3],
        'FMA': [2, 3, 4],
        'MAM': [3, 4, 5],
        'AMJ': [4, 5, 6],
        'MJJ': [5, 6, 7],
        'JJA': [6, 7, 8],
        'JAS': [7, 8, 9],
        'ASO': [8, 9, 10],
        'SON': [9, 10, 11],
        'OND': [10, 11, 12],
        'NDJ': [11, 12, 1]
    }

    if season in triplet_months:
        selected_months = triplet_months[season]
        return xr_data.sel(time=(xr_data['time.month'] == selected_months[0]) | (xr_data['time.month'] == selected_months[1]) | (xr_data['time.month'] == selected_months[2]))
    else:
        raise ValueError("Invalid season abbreviation. Please use one of the provided abbreviations.")

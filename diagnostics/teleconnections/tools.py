'''
This module contains simple tools for the teleconnections diagnostic.
- area selection functions, to deal with custom areas
- loading functions, to deal with yaml files
- conversion functions, to deal with conversion between different
  physical units.
- weighted area mean function, to deal with weighted area mean
'''
import numpy as np
from aqua.util import load_yaml, get_config_dir


def area_selection(indat, lat=None, lon=None, box_brd=True):
    """
        Extract a custom area from a DataArray.

        Args:
            indat (DataArray):      input data to be selected
            lat (list, opt):        latitude coordinates
            lon (list, opt):        longitude coordinates
            box_brd (bool,opt):     choose if coordinates are
                                    comprised or not.
                                    Default is True

        Returns:
            odat (DataArray):       data on a custom surface
    """
    # 1. -- Extract coordinates from indat --
    if lat is None and lon is None:
        raise ValueError('lat and lon cannot be both None')
    if lat:
        if lat[0] > lat[1]:
            raise ValueError('lat must be specified in ascending order')
        else:
            lat_coord = indat.lat
    if lon:
        if lon[0] > lon[1]:
            raise ValueError('lon must be specified in ascending order')
        else:
            lon_coord = indat.lon

    # 2. -- Select area --
    if box_brd:
        if lat:
            iplat = lat_coord.where((lat_coord >= lat[0]) &
                                    (lat_coord <= lat[1]), drop=True)
        if lon:
            iplon = lon_coord.where((lon_coord >= lon[0]) &
                                    (lon_coord <= lon[1]), drop=True)
    else:
        if lat:
            iplat = lat_coord.where((lat_coord > lat[0]) &
                                    (lat_coord < lat[1]), drop=True)
        if lon:
            iplon = lon_coord.where((lon_coord > lon[0]) &
                                    (lon_coord < lon[1]), drop=True)

    # 3. -- Are selection --
    odat = indat
    if lat:
        odat = odat.sel(lat=iplat)
    if lon:
        odat = odat.sel(lon=iplon)

    return odat


def load_namelist(diagname='teleconnections', configdir=None):
    """
    Load diagnostic yaml file.

    Args:
        diagname (str, opt):    diagnostic name. Default is 'teleconnections'
        configdir (str, opt):   path to config directory. Default is Nones

    Returns:
        namelist (dict):        Diagnostic configuration
    """
    if configdir is None:
        filename = f'{diagname}.yaml'
        configdir = get_config_dir(filename)

    infile = f'{configdir}/{diagname}.yaml'
    namelist = load_yaml(infile)

    return namelist


def lon_180_to_360(lon):
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


def lon_360_to_180(lon):
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
                  lonW: float, lonE: float, box_brd=True):
    """
    Evaluate the weighted mean of a quantity on a custom surface.

    Args:
        indat (DataArray):  input data to be averaged
        latN (float):       North latitude
        latS (float):       South latitude
        lonW (float):       West longitude
        lonE (float):       Est longitude
        box_brd (bool,opt): choose if coordinates are comprised or not.
                            Default is True

    Returns:
        odat (DataArray): average of input data on a custom surface
    """
    # 1. -- Extract coordinates from indat --
    lat = indat.lat

    # 2. -- Select area --
    indat = area_selection(indat, lat=[latS, latN],
                           lon=[lonW, lonE], box_brd=box_brd)

    # 3. -- Weighted area mean --
    wgt = np.cos(np.deg2rad(lat))
    odat = indat.weighted(wgt).mean(("lon", "lat"), skipna=True)

    return odat

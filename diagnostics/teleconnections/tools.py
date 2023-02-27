'''
This module contains simple tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
- conversion functions, to deal with conversion between different physical units.
'''
import yaml
import sys
import numpy as np
sys.path.insert(1, '../../')
from aqua import util

def load_config(machine='wilma',configdir='../../config/'):
    """
    Load machine config yaml file.

    Args:
        machine (str):   machine name
        configdir (str): path to config directory
    
    Returns:
        config (dict):  machine config
    """
    infile = f'{configdir}/config_{machine}.yml'
    config = util.load_yaml(infile)

    return config

def load_namelist(diagname='teleconnections',configdir='./'):
    """
    Load diagnostic yaml file.

    Args:
        diagname (str):  diagnostic name
        configdir (str): path to config directory
    
    Returns:
        namelist (dict): diagnostic config
    """
    infile = f'{configdir}/{diagname}.yml'
    namelist = util.load_yaml(infile)
    
    return namelist

def lon_180_to_360(lon):
    """
    Convert longitude [-180,180] to [0,360] range.

    Args:
        lon (float): longitude coordinate
    
    Returns:
        lon (float): converted longitude
    """
    if lon<0:
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
    if lon>180:
        lon = - 360 + lon
    return lon

def wgt_area_mean(indat,latN:float,latS:float,lonW:float,lonE:float,box_brd=True):
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
  lat=indat.lat
  lon=indat.lon
  
  # 2. -- Select area --
  if box_brd:
    iplat = lat.where( (lat >= latS ) & (lat <= latN), drop=True)
    iplon = lon.where( (lon >= lonW ) & (lon <= lonE), drop=True)
  else:
    iplat = lat.where( (lat > latS ) & (lat < latN), drop=True)
    iplon = lon.where( (lon > lonW ) & (lon < lonE), drop=True)

  # 3. -- Weighted area mean --
  wgt = np.cos(np.deg2rad(lat))
  odat=indat.sel(lat=iplat,lon=iplon).weighted(wgt).mean(("lon", "lat"), skipna=True)

  return odat
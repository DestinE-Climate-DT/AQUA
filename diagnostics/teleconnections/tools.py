'''
This module contains simple tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
- conversion functions, to deal with conversion between different physical units.
'''
import yaml
import numpy as np

def load_config(machine):
    """
    Load machine config yaml file.

    Args:
        machine (str): machine name
    
    Returns:
        config (dict):
    """
    with open(f'../../config/config_{machine}.yml', 'r', encoding='utf-8') as file:
        config = yaml.load(file, Loader=yaml.FullLoader)
    return config

def load_namelist(diagname):
    """
    Load diagnostic yaml file.

    Args:
        diagname (str): diagnostic name
    
    Returns:
        namelist (dict):
    """
    with open(f'{diagname}.yml', 'r', encoding='utf-8') as file:
        namelist = yaml.load(file, Loader=yaml.FullLoader)
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

def wgt_area_mean(indat, latN, latS, lonW, lonE):
  """ 
    Evaluate the weighted mean of a quantity on a custom surface.

    Args:
        indat (DataArray): input data to be averaged
        latN (float):      North latitude
        latS (float):      South latitude
        lonW (float):      West longitude
        lonE (float):      Est longitude

    Returns:
        odat (DataArray): average of input data on a custom surface
  """
  lat=indat.lat
  lon=indat.lon
  '''
  iplat = lat.where( (lat >= latS ) & (lat <= latN), drop=True)
  iplon = lon.where( (lon >= lonW ) & (lon <= lonE), drop=True)
  '''
  iplat = lat.where( (lat > latS ) & (lat < latN), drop=True)
  iplon = lon.where( (lon > lonW ) & (lon < lonE), drop=True)

  wgt = np.cos(np.deg2rad(lat))
  odat=indat.sel(lat=iplat,lon=iplon).weighted(wgt).mean(("lon", "lat"), skipna=True)
  return(odat)
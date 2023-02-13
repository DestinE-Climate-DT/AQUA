'''
This module contains simple functions for data plotting.
'''
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import xarray as xr

def cor_plot(indx,field,projection_type='PlateCarree',plot=True):
    """
    Evaluate and plot correlation map of a teleconnection index 
    and a DataArray field

    Args:
        indx (DataArray):       index DataArray
        field (DataArray):      field DataArray
        projection_type (str):  projection style for cartopy
                                If a wrong one is provided, it will fall back
                                to PlateCarree
        plot (bool):            enable or disable the plot output, true by default
    
    Returns:
        reg (DataArray): DataArray for correlation map
    """
    projection_types = {
        'PlateCarree': ccrs.PlateCarree(),
        'LambertConformal': ccrs.LambertConformal(),
        'Mercator': ccrs.Mercator()
    }

    # 1. -- Evaluate the map --
    cor = xr.corr(indx,field, dim="time")

    proj = projection_types.get(projection_type, ccrs.PlateCarree())

    if plot:
        fig, ax = plt.subplots(subplot_kw={'projection': proj},figsize=(8,4))
        ax.set_xlabel('longitude')
        ax.set_ylabel('latitude')
        ax.coastlines()
        cor.plot(ax=ax)

    return cor

def index_plot(indx):
    """
    Index plot together with red line at indx=0

    Args:
        indx (DataArray): Index DataArray
    
    Returns:
    """
    fig, ax = plt.subplots(figsize=(12, 8))
    
    indx.plot(ax=ax,ds='steps')
    ax.hlines(y=0,xmin=min(indx['time']),xmax=max(indx['time']),
              color='red')
    return

def reg_plot(indx,field,projection_type='PlateCarree',plot=True):
    """
    Evaluate and plot regression map of a teleconnection index 
    and a DataArray field

    Args:
        indx (DataArray):       index DataArray
        field (DataArray):      field DataArray
        projection_type (str):  projection style for cartopy
                                If a wrong one is provided, it will fall back
                                to PlateCarree
        plot (bool):            enable or disable the plot output, true by default
    
    Returns:
        reg (DataArray): DataArray for regression map
    """
    projection_types = {
        'PlateCarree': ccrs.PlateCarree(),
        'LambertConformal': ccrs.LambertConformal(),
        'Mercator': ccrs.Mercator()
    }
    reg = xr.cov(indx, field, dim="time")/indx.var(dim='time',skipna=True).values
    
    proj = projection_types.get(projection_type, ccrs.PlateCarree())

    if plot:
        fig, ax = plt.subplots(subplot_kw={'projection': proj},figsize=(8,4))
        ax.set_xlabel('longitude')
        ax.set_ylabel('latitude')
        ax.coastlines()
        reg.plot(ax=ax)
    
    return reg
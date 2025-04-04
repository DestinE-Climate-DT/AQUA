"""Graphics utilities for Aqua."""
import math

import xarray as xr
import cartopy.util as cutil
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import numpy as np
import matplotlib.pyplot as plt

from aqua.logger import log_configure
from .sci_util import check_coordinates


def add_cyclic_lon(da: xr.DataArray):
    """
    Add a cyclic longitude point to a DataArray using cartopy
    and preserving the DataArray as data structure.

    It assumes that the longitude coordinate is named 'lon' and
    the latitude coordinate is named 'lat'.

    Args:
        da (xarray.DataArray): Input data array with longitude coordinate

    Returns:
        xarray.DataArray: The same input data array with the cyclic point added along longitude
    """
    if not isinstance(da, xr.DataArray) or da is None:
        raise ValueError("Input must be an xarray.DataArray object.")

    # Support both lon and longitude names
    lon_name, lat_name = coord_names(da)

    cyclic_da, cyclic_lon = cutil.add_cyclic_point(da, coord=da[lon_name])

    # update the longitude coordinate with cyclic longitude
    new_da = xr.DataArray(cyclic_da, dims=da.dims)
    new_da = new_da.assign_coords(lon=cyclic_lon)
    new_da = new_da.assign_coords(lat=da[lat_name])

    # Add old attributes to the new DataArray
    new_da.attrs = da.attrs

    return new_da


def plot_box(num_plots=0):
    """
    Evaluate the number of rows and columns for a plot
    based on the number of plots to be plotted.

    Args:
        num_plots (int): Number of plots to be plotted.

    Returns:
        num_rows (int): Number of rows for the plot.
        num_cols (int): Number of columns for the plot.

    Raises:
        ValueError: If the number of plots is 0.
    """
    if num_plots == 0:
        raise ValueError('Number of plots must be greater than 0.')

    num_cols = math.ceil(math.sqrt(num_plots))
    num_rows = math.ceil(num_plots / num_cols)

    return num_rows, num_cols


def minmax_maps(maps: list):
    """
    Find the minimum and maximum values of the maps values
    for a list of maps.

    After finding the minimum and maximum values,
    values are made simmetric around 0.

    Args:
        regs (list): List of maps.

    Returns:
        vmin (float): Minimum value of the colorbar.
        vmax (float): Maximum value of the colorbar.
    """

    minmax = (min([map.min().values for map in maps if map is not None]),
              max([map.max().values for map in maps if map is not None]))

    vmin = minmax[0]
    vmax = minmax[1]

    # Make values simmetric around 0
    absmax = max(abs(vmin), abs(vmax))
    vmin = -absmax
    vmax = absmax

    return vmin, vmax


def evaluate_colorbar_limits(maps: list, sym: bool = True):
    """
    Evaluate the minimum and maximum values of the colorbar

    Args:
        maps (list):     List of DataArrays.
        sym (bool, opt): If True, the colorbar is symmetrical around 0.

    Returns:
        vmin (float): Minimum value of the colorbar.
        vmax (float): Maximum value of the colorbar.
    """
    if maps is None:
        raise ValueError('DataArrays must be specified.')

    if sym:
        vmin, vmax = minmax_maps(maps)
    else:
        vmin = min([map.min().values for map in maps if map is not None])
        vmax = max([map.max().values for map in maps if map is not None])

    return vmin, vmax


def cbar_get_label(data: xr.DataArray, cbar_label: str = None,
                   loglevel='WARNING'):
    """
    Evaluate the colorbar label.

    Args:
        data (xarray.DataArray): Input data array.
        cbar_label (str, opt):   Colorbar label.
        loglevel (str, opt):     Log level.

    Returns:
        cbar_label (str): Colorbar label.
    """
    logger = log_configure(loglevel, 'cbar get label')

    if cbar_label is None:
        cbar_label = getattr(data, 'long_name', None)
        if cbar_label is None:
            cbar_label = getattr(data, 'short_name', None)
        if cbar_label is None:
            cbar_label = getattr(data, 'shortName', None)
        logger.debug("Using %s as colorbar label", cbar_label)

        units = getattr(data, 'units', None)

        if units:
            cbar_label = f"{cbar_label} [{units}]"
            logger.debug("Adding units to colorbar label")

    if cbar_label is None:
        logger.warning("No colorbar label found, please specify one with the cbar_label argument.")

    return cbar_label


def set_map_title(data: xr.DataArray, title: str = None,
                  loglevel='WARNING'):
    """
    Evaluate the map title.

    Args:
        data (xarray.DataArray): Input data array.
        title (str, opt):        Map title.
        loglevel (str, opt):     Log level.

    Returns:
        title (str): Map title.
    """
    logger = log_configure(loglevel, 'set map title')

    if title is None:
        title = ""
        # Getting the variables from the possible attributes
        for attr in ['long_name', 'short_name', 'shortName']:
            varname = data.attrs[attr] if attr in data.attrs else None
            if varname is not None:
                break
        units = data.attrs['units'] if 'units' in data.attrs else None
        model = data.attrs["AQUA_model"] if 'AQUA_model' in data.attrs else None
        exp = data.attrs["AQUA_exp"] if 'AQUA_exp' in data.attrs else None
        time = data.time.values if 'time' in data.dims else None

        if varname:
            title += varname
            if units:
                title += f" [{units}]"
        if model is not None and exp is not None:
            title += f" {model} {exp}"
        if time is not None:
            time = np.datetime_as_string(time, unit='h')
            title += f" {time}"
        if title == "":
            logger.warning("No title found, please specify one with the title argument.")
            title = None
        else:
            logger.debug("Using %s as map title", title)

    return title


def coord_names(data: xr.DataArray):
    """
    Get the names of the longitude and latitude coordinates.

    Args:
        data (xarray.DataArray): Input data array.

    Returns:
        lon_name (str): Name of the longitude coordinate.
        lat_name (str): Name of the latitude coordinate.
    """
    try:
        lon_name = 'lon'
        data.lon
    except AttributeError:
        lon_name = 'longitude'
        data.longitude
    try:
        lat_name = 'lat'
        data.lat
    except AttributeError:
        lat_name = 'latitude'
        data.latitude

    return lon_name, lat_name


def ticks_round(ticks: list, round_to: int = None):
    """
    Round a tick to the nearest round_to value.

    Args:
        tick (list):          Tick value.
        round_to (int, opt):  Round to value.

    Returns:
        tick (list):  Rounded tick value.
    """
    if round_to is None:
        # define round_to
        tick_span = ticks[1] - ticks[0]
        if tick_span <= 1:
            round_to = 2
        elif tick_span > 1 and tick_span <= 10:
            round_to = 1
        else:
            round_to = 0

    return np.round(ticks, round_to)


def set_ticks(data: xr.DataArray,
              fig: plt.figure,
              ax: plt.axes,
              nticks: tuple,
              lon_name: str,
              lat_name: str,
              ticks_rounding: int = None,
              proj=ccrs.PlateCarree(),
              loglevel='WARNING'):
    """
    Set the ticks of the map.

    Args:
        data (xr.DataArray): Data to plot.
        fig (matplotlib.figure.Figure): Figure.
        ax (matplotlib.axes._subplots.AxesSubplot): Axes.
        nticks (tuple): Number of ticks for x and y axes.
        lon_name (str): Name of the longitude coordinate.
        lat_name (str): Name of the latitude coordinate.
        ticks_rounding (int, optional): Number of digits to round the ticks.
        loglevel (str, optional): Log level. Defaults to 'WARNING'.

    Returns:
        matplotlib.figure.Figure, matplotlib.axes._subplots.AxesSubplot: Figure and axes.
    """
    logger = log_configure(loglevel, 'set_ticks')
    nxticks, nyticks = nticks

    try:
        lon_min = data[lon_name].values.min()
        lon_max = data[lon_name].values.max()
        (lon_min, lon_max), _ = check_coordinates(lon=(lon_min, lon_max),
                                                  default={"lon_min": -180,
                                                           "lon_max": 180,
                                                           "lat_min": -90,
                                                           "lat_max": 90},)
        logger.debug("Setting longitude ticks from %s to %s", lon_min, lon_max)
    except KeyError:
        logger.critical("No longitude coordinate found, setting default values")
        lon_min = -180
        lon_max = 180
    step = (lon_max - lon_min) / (nxticks - 1)
    xticks = np.arange(lon_min, lon_max + 1, step)
    xticks = ticks_round(ticks=xticks, round_to=ticks_rounding)
    logger.debug("Setting longitude ticks to %s", xticks)
    ax.set_xticks(xticks, crs=proj)
    lon_formatter = cticker.LongitudeFormatter()
    ax.xaxis.set_major_formatter(lon_formatter)

    # Latitude labels
    # Evaluate the latitude ticks
    try:
        lat_min = data[lat_name].values.min()
        lat_max = data[lat_name].values.max()
        _, (lat_min, lat_max) = check_coordinates(lat=(lat_min, lat_max),
                                                  default={"lon_min": -180,
                                                           "lon_max": 180,
                                                           "lat_min": -90,
                                                           "lat_max": 90},)
        logger.debug("Setting latitude ticks from %s to %s", lat_min, lat_max)
    except KeyError:
        logger.critical("No latitude coordinate found, setting default values")
        lat_min = -90
        lat_max = 90
    step = (lat_max - lat_min) / (nyticks - 1)
    yticks = np.arange(lat_min, lat_max + 1, step)
    yticks = ticks_round(ticks=yticks, round_to=ticks_rounding)
    logger.debug("Setting latitude ticks to %s", yticks)
    ax.set_yticks(yticks, crs=proj)
    lat_formatter = cticker.LatitudeFormatter()
    ax.yaxis.set_major_formatter(lat_formatter)

    return fig, ax

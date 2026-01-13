"""Graphics utilities for Aqua."""
import math

import xarray as xr
import cartopy.util as cutil
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import numpy as np
import healpy as hp
from scipy.interpolate import griddata
import matplotlib.pyplot as plt
import matplotlib.path as mpath
import matplotlib.patches as mpatches

from aqua.core.logger import log_configure
from .sci_util import check_coordinates
from .string import unit_to_latex


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
            cbar_label = f"{cbar_label} [{unit_to_latex(units)}]"
            logger.debug("Adding units to colorbar label")

    if cbar_label is None:
        logger.warning("No colorbar label found, please specify one with the cbar_label argument.")

    return cbar_label


def set_map_title(data: xr.DataArray, title: str = None,
                  put_units=False, set_units=None,
                  use_attr_name=None, put_model_name=True,
                  put_exp_name=True, skip_varname=False,
                  loglevel='WARNING'):
    """
    Evaluate the map title.

    Args:
        data (xarray.DataArray):   Input data array.
        title (str, opt):          Explicit title override.
        put_units (bool):      Whether to include units in the title. Default True.
        set_units (str):       Manually override units in title.
        use_attr_name (str):   Specific attribute name to use as title.
        put_model_name (bool): Include 'AQUA_model' in title. Default True.
        put_exp_name (bool):   Include 'AQUA_exp' in title. Default True.
        skip_varname (bool):   Skip including variable name in title. Default False.
        loglevel (str, opt):       Logging level.

    Returns:
        title (str): Map title.
    """
    logger = log_configure(loglevel, 'set map title')

    if title:
        logger.debug("Explicit title provided: %s", title)
        return title
    
    title = ""
    varname = None

    if use_attr_name:
        varname = data.attrs.get(use_attr_name, None)
        if varname:
            logger.debug(f"Using title from specified attribute '{use_attr_name}': {varname}")
        else:
            logger.warning(f"Attribute '{use_attr_name}' not found in data attributes. Checking standard ones")

    if varname is None:
        # Getting the variables from the possible attrs in data
        for attr in ['long_name', 'short_name', 'shortName']:
            varname = data.attrs[attr] if attr in data.attrs else None
            if varname is not None:
                break

    units = set_units if set_units is not None else data.attrs.get('units', None)
    model = data.attrs["AQUA_model"] if 'AQUA_model' in data.attrs else None
    exp = data.attrs["AQUA_exp"] if 'AQUA_exp' in data.attrs else None
    time = data.time.values if 'time' in data.dims else None

    if varname and not skip_varname:
        title += varname
        if units and put_units:
            title += f" [{unit_to_latex(units)}]"
    if put_model_name and model:
            title += f" {model}"
    if put_exp_name and exp:
            title += f" {exp}"
    if time is not None:
        time = np.datetime_as_string(time, unit='h')
        title += f" {time}"
    if title == "":
        logger.warning("No title found, please specify one with the title argument.")
        title = None
    else:
        logger.debug(f"Using {title} as map title") 
    return title


def coord_names(data: xr.DataArray):
    """
    Get the names of the longitude and latitude coordinates.

    Args:
        data (xarray.DataArray): Input data array.

    Returns:
        tuple: (lon_name, lat_name) - Names of longitude and latitude coordinates, 
               or (None, None) if not found.
    """
    lon_name = None
    lat_name = None
    
    # Find longitude coordinate
    for lon_candidate in ['lon', 'longitude']:
        if lon_candidate in data.coords:
            lon_name = lon_candidate
            break
    
    # Find latitude coordinate  
    for lat_candidate in ['lat', 'latitude']:
        if lat_candidate in data.coords:
            lat_name = lat_candidate
            break
    
    return lon_name, lat_name


def ticks_round(ticks: list, round_to: int = None):
    """
    Round ticks to the nearest round_to value.

    Args:
        ticks (list):         Ticks value.
        round_to (int, opt):  Round to value.

    Returns:
        ticks (list):  Rounded tick value.
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
                                                  lat=None,
                                                  default_coords={"lon_min": -180,
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
                                                  lon=None,
                                                  default_coords={"lon_min": -180,
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

def generate_colorbar_ticks(vmin, vmax, nlevels=11, sym=False,
                            ticks_rounding=None, max_ticks=15, loglevel='WARNING'):
    """
    Generate and optionally round colorbar ticks for consistent and readable display.

    Args:
        vmin (float): Minimum colorbar value.
        vmax (float): Maximum colorbar value.
        nlevels (int): Number of desired levels. Default is 11.
        sym (bool): Whether the colorbar should be symmetric around zero.
        ticks_rounding (int, optional): Decimal places for rounding. Default is None.
        max_ticks (int): Max allowed ticks on the colorbar. Default is 15.
        loglevel (str): Logging level.

    Returns:
        np.ndarray: Array of colorbar tick positions.
    """
    logger = log_configure(loglevel, 'generate_colorbar_ticks')

    if sym:
        vmax = max(abs(vmin), abs(vmax))
        vmin = -vmax
        logger.debug(f"Using symmetric colorbar: {vmin=}, {vmax=}")

    cbar_ticks = np.linspace(vmin, vmax, nlevels + 1)

    # Reduce number of ticks if too many
    if len(cbar_ticks) > max_ticks:
        step = max(1, int(np.ceil(len(cbar_ticks) / max_ticks)))
        cbar_ticks = cbar_ticks[::step]
        # Ensure last tick is included
        if cbar_ticks[-1] != vmax:
            cbar_ticks = np.append(cbar_ticks, vmax)

    if ticks_rounding is not None:
        logger.debug(f"Rounding colorbar ticks to {ticks_rounding} decimals")
        cbar_ticks = ticks_round(cbar_ticks, ticks_rounding)

    return cbar_ticks

def apply_circular_window(ax, extent=None, apply_black_circle=False):
    """
    Apply a circular boundary mask to a Cartopy GeoAxes and set geographic extent
    to avoid the default rectangular plotting window with some projections.

    Args:
        ax (GeoAxes): Cartopy axes object to modify.
        extent (list or None): Geographic extent [west, east, south, north]. Default is Arctic region.

    Returns:
        ax (GeoAxes): Modified axes with circular boundary and extent.
    """
    if extent is None:
        extent = [-180, 180, 10, 90] # [west, east, south, north]

    # create circular boundary path
    theta = np.linspace(0, 2 * np.pi, 100)
    center, radius = [0.5, 0.5], 0.5
    verts = np.vstack([np.sin(theta), np.cos(theta)]).T
    circle = mpath.Path(verts * radius + center)

    # apply circle to axis
    ax.set_boundary(circle, transform=ax.transAxes)
    ax.set_extent(extent, crs=ccrs.PlateCarree())

    if apply_black_circle:
        # overlay a more visible black circle outline (drawn in axes coordinates)
        circle_patch = mpatches.Circle(
            center, radius=radius, transform=ax.transAxes,
            fill=False, color='darkgrey', linewidth=1.5, zorder=10)
        ax.add_patch(circle_patch)
    return ax

"""
Following functions are taken and adjusted from the easygems package,
on this repository:

https://github.com/mpimet/easygems/blob/main/easygems/healpix/__init__.py
"""
def get_nside(data):
    """
    Get the nside of a HEALPix map.

    Args:
        data (numpy.ndarray or xarray.DataArray): HEALPix map.
    
    Returns:
        int: nside of the HEALPix map.
    
    Raises:
        ValueError: If the input data is not a valid HEALPix map.
    """
    # Check if the input is a numpy array or xarray DataArray
    if not isinstance(data, (np.ndarray, xr.DataArray)): 
        raise ValueError("Input data must be a numpy array or xarray DataArray")

    if data.size == 0:  # Check for empty data
        raise ValueError("Invalid HEALPix map: data array is empty")
    
    npix = data.size
    if not hp.isnpixok(npix):
        raise ValueError(f"Invalid HEALPix map: npix={npix}")
    return hp.npix2nside(npix)
    
def get_npix(data):
    """
    Get the number of pixels in a HEALPix map based on the map data.

    Args:
        data (numpy.ndarray or xarray.DataArray): HEALPix map data.

    Returns:
        int: Number of pixels in the HEALPix map.
    """
    return hp.nside2npix(get_nside(data))
    
def healpix_resample(
        var,
        xlims=None,
        ylims=None,
        nx=None,
        ny=None,
        src_crs=None,
        method="nearest",
        nest=True,
        nside_out=None,
        loglevel="WARNING",
        ):
    """
    Resample a HEALPix map to a lat/lon grid.

    Args:
        var (xarray.DataArray): Input HEALPix map.
        xlims (tuple, optional): Longitude limits for the output grid.
        ylims (tuple, optional): Latitude limits for the output grid.
        nx (int, optional): Number of points in the x direction.
        ny (int, optional): Number of points in the y direction.
        src_crs (cartopy.crs.Projection, optional): Source coordinate reference system.
        method (str, optional): Resampling method ('nearest' or 'linear').
        nest (bool, optional): Whether to use nested HEALPix scheme.
        nside_out (int, optional): Output HEALPix nside.
        loglevel (str, optional): Log level.

    Returns:
        xarray.DataArray: Resampled data on a lat/lon grid.
    """

    logger = log_configure(loglevel, "healpix resample")

    nside = get_nside(var)
    if nside_out is None:
        nside_out = nside

    resolution_deg = 58.6 / nside_out
    if xlims is None:
        xlims = (-180, 180)
    if ylims is None:
        ylims = (-90, 90)

    if nx is None:
        nx = int((xlims[1] - xlims[0]) / resolution_deg)
    if ny is None:
        ny = int((ylims[1] - ylims[0]) / resolution_deg)

    if src_crs is None:
        src_crs = ccrs.Geodetic()

    # Compute grid centers
    dx = (xlims[1] - xlims[0]) / nx
    dy = (ylims[1] - ylims[0]) / ny
    xvals = np.linspace(xlims[0] + dx / 2, xlims[1] - dx / 2, nx)
    yvals = np.linspace(ylims[0] + dy / 2, ylims[1] - dy / 2, ny)
    xvals2, yvals2 = np.meshgrid(xvals, yvals)

    # Transform to lat/lon
    latlon = ccrs.PlateCarree().transform_points(
        src_crs, xvals2, yvals2, np.zeros_like(xvals2)
    )
    valid = np.all(np.isfinite(latlon), axis=-1)
    points = latlon[valid].T

    res = np.full(latlon.shape[:-1], np.nan, dtype=var.dtype)

    logger.debug("Resampling HEALPix map to lat/lon grid with %d x %d points", nx, ny)
    if method == "nearest":
        pix = hp.ang2pix(
            get_nside(var),
            theta=points[0],
            phi=points[1],
            nest=nest,
            lonlat=True,
        )
        if var.size < get_npix(var):
            if not isinstance(var, xr.DataArray):
                raise ValueError(
                    "Sparse HEALPix grids are only supported as xr.DataArray"
                )
            res[valid] = var.sel(cell=pix, method="nearest").where(
                lambda x: x.cell == pix
            )
        else:
            res[valid] = var[pix]

    elif method == "linear":
        lons, lats = hp.pix2ang(
            nside=get_nside(var),
            ipix=np.arange(len(var)),
            nest=nest,
            lonlat=True,
        )
        lons = (lons + 180) % 360 - 180

        valid_src = ((lons > points[0].min()) & (lons < points[0].max())) | (
            (lats > points[1].min()) & (lats < points[1].max())
        )

        res[valid] = griddata(
            points=np.asarray([lons[valid_src], lats[valid_src]]).T,
            values=var[valid_src],
            xi=(points[0], points[1]),
            method="linear",
            fill_value=np.nan,
            rescale=True,
        )

    result = xr.DataArray(res, coords=[("lat", yvals), ("lon", xvals)])
    result.attrs = getattr(var, "attrs", {}).copy()
    return result

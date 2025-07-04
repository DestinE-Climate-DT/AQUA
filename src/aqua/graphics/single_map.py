"""
Module to plot a single map of a variable.
Contains the following functions:

    - plot_single_map: Plot a single map of a variable.
    - plot_single_map_diff: Plot the difference of two variables as a map and add the data as a contour plot.

Author: Matteo Nurisso
Date: Feb 2024
"""
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import healpy as hp
from aqua.logger import log_configure
from aqua.util import add_cyclic_lon, evaluate_colorbar_limits
from aqua.util import healpix_resample
from aqua.util import cbar_get_label, set_map_title
from aqua.util import coord_names, set_ticks, ticks_round
from aqua.exceptions import NoDataError
from .styles import ConfigStyle

def plot_single_map(data: xr.DataArray,
                    contour=True, sym=False,
                    proj: ccrs.Projection = ccrs.Robinson(),
                    extent=None, coastlines=True,
                    style=None, figsize=(11, 8.5), nlevels=11,
                    vmin=None, vmax=None, cmap='RdBu_r',
                    cbar: bool = True, cbar_label=None,
                    title=None, transform_first=False, cyclic_lon=True,
                    fig: plt.Figure = None, ax: plt.Axes = None,
                    ax_pos: tuple = (1, 1, 1),
                    return_fig=False, loglevel='WARNING',  **kwargs):
    """
    Plot contour or pcolormesh map of a single variable. By default the contour map is plotted.

    Args:
        data (xr.DataArray):         Data to plot.
        contour (bool, optional):    If True, plot a contour map, otherwise a pcolormesh. Defaults to True.
        sym (bool, optional):        If True, set the colorbar to be symmetrical. Defaults to False.
        proj (cartopy.crs.Projection, optional): Projection to use. Defaults to PlateCarree.
        extent (list, optional):     Extent of the map to limit the projection. Defaults to None.
        coastlines (bool, optional): If True, add coastlines. Defaults to True.
        style (str, optional):       Style to use. Defaults to None (aqua style).
        figsize (tuple, optional):   Figure size. Defaults to (11, 8.5).
        nlevels (int, optional):     Number of levels for the contour map. Defaults to 11.
        vmin (float, optional):      Minimum value for the colorbar. Defaults to None.
        vmax (float, optional):      Maximum value for the colorbar.
                                     Defaults to None.
        cmap (str, optional):        Colormap. Defaults to 'RdBu_r'.
        cbar (bool, optional):      If True, add a colorbar. Defaults to True.
        cbar_label (str, optional):  Colorbar label. Defaults to None.
        title (str, optional):       Title of the figure. Defaults to None.
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to False.
        cyclic_lon (bool, optional): If True, add cyclic longitude. Defaults to True.
        fig (plt.Figure, optional):  Figure to plot on. By default a new figure is created.
        ax (plt.Axes, optional):    Axes to plot on. By default a new axes is created.
        ax_pos (list, optional):  Axes position. Used if the axes has to be created. Defaults to (1, 1, 1).
        return_fig (bool, optional): If True, return the figure and axes. Defaults to False.
        loglevel (str, optional):    Log level. Defaults to 'WARNING'.

    Keyword Args:
        nxticks (int, optional):     Number of x ticks. Defaults to 7.
        nyticks (int, optional):     Number of y ticks. Defaults to 7.
        ticks_rounding (int, optional):  Number of digits to round the ticks.
                                         Defaults to 0 for full map, 1 if min-max < 10,
                                         2 if min-max < 1.
        cbar_ticks_rounding (int, optional): Number of digits to round the colorbar ticks.
                                            Default is no rounding.

    Returns:
        tuple: Figure and axes.
    """
    logger = log_configure(loglevel, 'plot_single_map')
    ConfigStyle(style=style, loglevel=loglevel)


    # Check if the data is in HEALPix format
    npix = data.size  # Number of cells in the data
    nside = hp.npix2nside(npix) if hp.isnpixok(npix) else None

    if nside is not None:
        logger.info(f"Input data is in HEALPix format with nside={nside}.")
        data = healpix_resample(data)

    # We load in memory the data, to speed up the plotting, Dask is slow with matplotlib
    logger.debug("Loading data in memory")
    data = data.load(keep_attrs=True)

    if cyclic_lon:
        logger.debug("Adding cyclic longitude")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)

    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(ax_pos[0], ax_pos[1], ax_pos[2], projection=proj)

    # For certain projections, we may need to set the extent
    if extent:
        logger.debug("Setting extent to %s", extent)
        ax.set_extent(extent, ccrs.PlateCarree())

    # Evaluate vmin and vmax if not given
    if vmin is None or vmax is None:
        vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    else:
        if sym:
            logger.warning("sym=True, vmin and vmax given will be ignored")
            vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)
    if contour:
        levels = np.linspace(vmin, vmax, nlevels + 1)

    if np.allclose(data, 0):
        logger.error("The map is zero! You are trying to plot an empty dataset")
        contour = False  # Disable contour if map is zero

    # Plot the data
    if contour:
        try:
            cs = data.plot.contourf(ax=ax,
                                    transform=ccrs.PlateCarree(),
                                    cmap=cmap,
                                    vmin=vmin, vmax=vmax,
                                    levels=levels,
                                    extend='both',
                                    transform_first=transform_first,
                                    add_colorbar=False)
        except ValueError as e:
            logger.error("Cannot plot contourf: %s", e)
            logger.warning(f"Trying with transform_first={not transform_first}")
            cs = data.plot.contourf(ax=ax,
                                    transform=ccrs.PlateCarree(),
                                    cmap=cmap,
                                    vmin=vmin, vmax=vmax,
                                    levels=levels,
                                    extend='both',
                                    transform_first=not transform_first,
                                    add_colorbar=False)
    else:
        cs = data.plot.pcolormesh(ax=ax,
                                  transform=ccrs.PlateCarree(),
                                  cmap=cmap,
                                  vmin=vmin, vmax=vmax,
                                  add_colorbar=False)

    if coastlines:
        logger.debug("Adding coastlines")
        ax.coastlines()

    # TODO: To reimplement, we need a meshgrid for this
    # if gridlines:
    #     logger.debug("Adding gridlines")
    #     ax.gridlines()

    # Longitude labels
    # Evaluate the longitude ticks
    if proj == ccrs.PlateCarree():
        lon_name, lat_name = coord_names(data)
        nxticks = kwargs.get('nxticks', 7)
        nyticks = kwargs.get('nyticks', 7)
        ticks_rounding = kwargs.get('ticks_rounding', None)
        if ticks_rounding:
            logger.debug("Setting ticks rounding to %s", ticks_rounding)

        fig, ax = set_ticks(data=data, fig=fig, ax=ax, nticks=(nxticks, nyticks),
                            ticks_rounding=ticks_rounding, lon_name=lon_name,
                            lat_name=lat_name, proj=proj, loglevel=loglevel)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    if cbar:
        # Add a colorbar axis at the bottom of the graph
        cbar_ax = fig.add_axes([0.1, 0.15, 0.8, 0.02])

        cbar_label = cbar_get_label(data, cbar_label=cbar_label, loglevel=loglevel)
        logger.debug("Setting colorbar label to %s", cbar_label)

        cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal', label=cbar_label)

        # Make tick of colorbar symmetric if sym=True
        cbar_ticks_rounding = kwargs.get('cbar_ticks_rounding', None)
        if sym:
            logger.debug("Setting colorbar ticks to be symmetrical")
            cbar_ticks = np.linspace(-vmax, vmax, nlevels + 1)
        else:
            cbar_ticks = np.linspace(vmin, vmax, nlevels + 1)

        # If too many ticks, select a subset for readability
        max_ticks = 15
        if len(cbar_ticks) > max_ticks:
            step = max(1, int(np.ceil(len(cbar_ticks) / max_ticks)))
            cbar_ticks = cbar_ticks[::step]
            # Ensure last tick is included
            if cbar_ticks[-1] != (vmax if not sym else vmax):
                cbar_ticks = np.append(cbar_ticks, vmax if not sym else vmax)

        if cbar_ticks_rounding is not None:
            logger.debug("Setting colorbar ticks rounding to %s", cbar_ticks_rounding)
            cbar_ticks = ticks_round(cbar_ticks, cbar_ticks_rounding)
        cbar.set_ticks(cbar_ticks)
        cbar.ax.ticklabel_format(style='sci', axis='x', scilimits=(-3, 3))

    # Set x-y labels
    ax.set_xlabel('Longitude [deg]')
    ax.set_ylabel('Latitude [deg]')

    # Set title
    title = set_map_title(data, title=title, loglevel=loglevel)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax


def plot_single_map_diff(data: xr.DataArray, data_ref: xr.DataArray,
                         proj: ccrs.Projection = ccrs.Robinson(), extent: list = None,
                         vmin_fill: float = None, vmax_fill: float = None,
                         vmin_contour: float = None, vmax_contour: float = None,
                         sym_contour: bool = False, sym: bool = True,
                         cyclic_lon: bool = True, return_fig: bool = False,
                         fig: plt.Figure = None, ax: plt.Axes = None,
                         title: str = None, loglevel: str = 'WARNING', **kwargs):
    """
    Plot the difference of data-data_ref as map and add the data
    as a contour plot.

    Args:
        data (xr.DataArray):       Data to plot.
        data_ref (xr.DataArray):   Reference data to plot the difference.
        proj (cartopy.crs.Projection, optional): Projection to use. Defaults to PlateCarree.
        extent (list, optional):     Extent of the map to limit the projection. Defaults to None.
        vmin_fill (float, optional): Minimum value for the colorbar of the fill.
        vmax_fill (float, optional): Maximum value for the colorbar of the fill.
        vmin_contour (float, optional): Minimum value for the colorbar of the contour.
        vmax_contour (float, optional): Maximum value for the colorbar of the contour.
        sym_contour (bool, optional): If True, set the contour levels to be symmetrical.  Default to False
        sym (bool, optional):      If True, set the colorbar for the diff to be symmetrical. Default to True
        title (str, optional):     Title of the figure. Defaults to None.
        cyclic_lon (bool, optional): If True, add cyclic longitude. Defaults to True.
        return_fig (bool, optional): If True, return the figure and axes. Defaults to False.
        fig (plt.Figure, optional):  Figure to plot on. By default a new figure is created.
        ax (plt.Axes, optional):    Axes to plot on. By default a new axes is created.
        loglevel (str, optional):  Log level. Defaults to 'WARNING'.
        **kwargs:                  Keyword arguments for plot_single_map.
                                   Check the docstring of plot_single_map.

    Keyword Args:
        contour (bool, optional):  Plot the difference as contour. False to plot a pcolormesh
        coastlines (bool, optional): If True, add coastlines. Defaults to True.

    Raise:
        ValueError: If data or data_ref is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map_diff')


    # Check if the data is in HEALPix format
    npix = data.size  # Number of cells in the data
    nside = hp.npix2nside(npix) if hp.isnpixok(npix) else None

    if nside is not None:
        logger.info(f"Input data is in HEALPix format with nside={nside}.")
        data = healpix_resample(data)
        logger.debug("resampling HEALPix data")


    # Check if the data is in HEALPix format
    npix_ref = data_ref.size  # Number of cells in the data
    nside_ref = hp.npix2nside(npix_ref) if hp.isnpixok(npix_ref) else None

    if nside_ref is not None:
        logger.info(f"Reference data is in HEALPix format with nside={nside_ref}.")
        data_ref = healpix_resample(data_ref)
        logger.debug("resampling HEALPix data_ref")

    if isinstance(data_ref, xr.DataArray) is False or isinstance(data, xr.DataArray) is False:
        raise ValueError("Both data and data_ref must be an xarray.DataArray")

    # Evaluate the difference
    diff_map = data - data_ref

    if np.array_equal(np.nan_to_num(data.values), np.nan_to_num(data_ref.values)):
        logger.warning("The values are exactly the same (ignoring NaNs), no difference to plot")
        fig = plt.figure(figsize=kwargs.get('figsize', (11, 8.5)))
        ax = fig.add_subplot(111, projection=proj)
    else:
        fig, ax = plot_single_map(diff_map, cyclic_lon=cyclic_lon,
                                  proj=proj, extent=extent,
                                  fig=fig, ax=ax,
                                  sym=sym, vmin=vmin_fill, vmax=vmax_fill,
                                  loglevel=loglevel, return_fig=True, **kwargs)

    logger.debug("Plotting the map")
    data = data.load(keep_attrs=True)

    if cyclic_lon:
        logger.debug("Adding cyclic longitude to the difference map")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)
            logger.warning("Cyclic longitude can be set to False with cyclic_lon")

    logger.debug("Plotting the map as contour")

    # Evaluate vmin and vmax of the contour
    if vmin_contour is None or vmax_contour is None:
        vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[data], sym=sym_contour)
    else:
        if sym_contour:
            logger.warning("sym_contour=True, vmin_map and vmax_map given will be ignored")
            vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[data], sym=sym_contour)

    logger.debug("Setting contour vmin to %s, vmax to %s", vmin_contour, vmax_contour)

    ds = data.plot.contour(ax=ax,
                           transform=ccrs.PlateCarree(),
                           vmin=vmin_contour, vmax=vmax_contour,
                           levels=10, colors='k',
                           linewidths=0.5)

    fmt = {level: f"{level:.1e}" if (abs(level) < 0.1 or abs(level) > 1000) else f"{level:.1f}" for level in ds.levels}
    ax.clabel(ds, fmt=fmt, fontsize=6, inline=True)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

    if return_fig:
        return fig, ax

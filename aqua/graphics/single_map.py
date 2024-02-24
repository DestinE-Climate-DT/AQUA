"""
Module to plot a single map of a variable.
Contains the following functions:

    - plot_single_map: Plot a single map of a variable.
    - plot_single_map_diff: Plot the difference of two variables as a map and add the data as a contour plot.

Author: Matteo Nurisso
Date: Feb 2024
"""
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.logger import log_configure
from aqua.util import create_folder, ticks_round
from aqua.util import add_cyclic_lon, evaluate_colorbar_limits
from aqua.util import cbar_get_label, set_map_title
from aqua.util import check_coordinates, coord_names


def plot_single_map(data: xr.DataArray,
                    save=False,
                    contour=True, sym=False,
                    figsize=(11, 8.5),
                    nlevels=11, outputdir=".",
                    vmin=None, vmax=None,
                    cmap='RdBu_r',
                    gridlines=False,
                    display=True,
                    return_fig=False,
                    loglevel='WARNING',
                    **kwargs):
    """
    Plot contour or pcolormesh map of a single variable.

    Args:
        data (xr.DataArray):       Data to plot.
        save (bool, optional):     If True, save the figure. Defaults to False.
        contour (bool, optional):  If True, plot a contour map,
                                   otherwise a pcolormesh.
                                   Defaults to True.
        figsize (tuple, optional): Figure size. Defaults to (11, 8.5).
        nlevels (int, optional):   Number of levels for the contour map.
                                   Defaults to 11.
        outputdir (str, optional): Output directory. Defaults to ".".
        vmin (float, optional):    Minimum value for the colorbar.
                                   Defaults to None.
        vmax (float, optional):    Maximum value for the colorbar.
                                   Defaults to None.
        cmap (str, optional):      Colormap. Defaults to 'RdBu_r'.
        gridlines (bool, optional): If True, plot gridlines. Defaults to False.
        display (bool, optional):  If True, display the figure. Defaults to True.
        return_fig (bool, optional): If True, return the figure (fig, ax). Defaults to False.
        loglevel (str, optional):  Log level. Defaults to 'WARNING'.

    Keyword Args:
        title (str, optional):       Title of the figure. Defaults to None.
        transform_first (bool, optional): If True, transform the data before
                                          plotting. Defaults to False.
        cbar_label (str, optional):  Colorbar label. Defaults to None.
        dpi (int, optional):         Dots per inch. Defaults to 100.
        model (str, optional):       Model name. Defaults to None.
        exp (str, optional):         Experiment name. Defaults to None.
        filename (str, optional):    Filename. Defaults to 'map'.
        format (str, optional):      Format of the figure. Defaults to 'pdf'.
        nxticks (int, optional):     Number of x ticks. Defaults to 7.
        nyticks (int, optional):     Number of y ticks. Defaults to 7.
        ticks_rounding (int, optional):  Number of digits to round the ticks.
                                         Defaults to 0 for full map, 1 if min-max < 10,
                                         2 if min-max < 1.
        cyclic_lon (bool, optional): If True, add cyclic longitude.

    Raises:
        ValueError: If data is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map')

    # We load in memory the data, to avoid problems with dask
    logger.info("Loading data in memory")
    data = data.load(keep_attrs=True)

    cycling = kwargs.get('cyclic_lon', True)
    if cycling:
        logger.info("Adding cyclic longitude")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)
            logger.warning("Cyclic longitude can be set to False with the cyclic_lon kwarg")

    proj = ccrs.PlateCarree()

    logger.debug("Setting figsize to %s", figsize)
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection=proj)

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

    # Get the coordinate names
    lon_name, lat_name = coord_names(data)

    # Plot the data
    if contour:
        # grid lon and lat
        lon, lat = np.meshgrid(data[lon_name], data[lat_name])

        transform_first = kwargs.get('transform_first', False)
        cs = ax.contourf(lon, lat, data, cmap=cmap,
                         transform=proj, levels=levels,
                         extend='both',
                         transform_first=transform_first)
    else:
        cs = ax.pcolormesh(data[lon_name], data[lat_name], data, cmap=cmap,
                           transform=proj, vmin=vmin, vmax=vmax)

    logger.debug("Adding coastlines")
    ax.coastlines()

    if gridlines:
        logger.debug("Adding gridlines")
        ax.gridlines()

    # Longitude labels
    # Evaluate the longitude ticks
    nxticks = kwargs.get('nxticks', 7)
    nyticks = kwargs.get('nyticks', 7)
    ticks_rounding = kwargs.get('ticks_rounding', None)
    if ticks_rounding:
        logger.debug("Setting ticks rounding to %s", ticks_rounding)

    fig, ax = _set_ticks(data=data, fig=fig, ax=ax, nticks=(nxticks, nyticks),
                         ticks_rounding=ticks_rounding, lon_name=lon_name,
                         lat_name=lat_name, proj=proj, loglevel=loglevel)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    cbar_label = cbar_get_label(data,
                                cbar_label=kwargs.get('cbar_label', None),
                                loglevel=loglevel)
    logger.debug("Setting colorbar label to %s", cbar_label)

    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make tick of colorbar simmetric if sym=True
    if sym:
        logger.debug("Setting colorbar ticks to be symmetrical")
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
    else:
        cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

    # Set x-y labels
    ax.set_xlabel('Longitude [deg]')
    ax.set_ylabel('Latitude [deg]')

    # Set title
    title = set_map_title(data, title=kwargs.get('title', None),
                          model=kwargs.get('model', None),
                          exp=kwargs.get('exp', None),
                          loglevel=loglevel)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

    # Saving
    if save:
        logger.debug("Saving figure to %s", outputdir)
        create_folder(outputdir, loglevel=loglevel)
        filename = kwargs.get('filename', 'map')
        plot_format = kwargs.get('format', 'pdf')
        if filename.endswith(plot_format):
            logger.debug("Format already set in the filename")
        else:
            filename = f"{filename}.{plot_format}"
        logger.debug("Setting filename to %s", filename)

        logger.info("Saving figure as %s/%s", outputdir, filename)
        if contour:
            dpi = kwargs.get('dpi', 300)
        else:
            dpi = kwargs.get('dpi', 100)
            if dpi == 100:
                logger.info("Setting dpi to 100 by default, use dpi kwarg to change it")

        fig.savefig('{}/{}'.format(outputdir, filename),
                    dpi=dpi, bbox_inches='tight')

    if display is False:
        logger.debug("Display is set to False, closing figure")
        plt.close(fig)

    if return_fig:
        logger.debug("Returning figure and axes")
        return fig, ax


def plot_single_map_diff(data: xr.DataArray,
                         data_ref: xr.DataArray,
                         vmin_fill=None, vmax_fill=None,
                         vmin_contour=None, vmax_contour=None,
                         save=False, display=True,
                         sym_contour=False, sym=True,
                         outputdir='.', filename='map.png',
                         title=None, loglevel='WARNING',
                         **kwargs):
    """
    Plot the difference of data-data_ref as map and add the data
    as a contour plot.

    Args:
        data (xr.DataArray):       Data to plot.
        data_ref (xr.DataArray):   Reference data to plot the difference.
        vmin_fill (float, optional): Minimum value for the colorbar of the fill.
        vmax_fill (float, optional): Maximum value for the colorbar of the fill.
        vmin_contour (float, optional): Minimum value for the colorbar of the contour.
        vmax_contour (float, optional): Maximum value for the colorbar of the contour.
        save (bool, optional):     If True, save the figure. Defaults to False.
        display (bool, optional):  If True, display the figure. Defaults to True.
        sym_contour (bool, optional): If True, set the contour levels to be symmetrical.
                                      Default to False
        sym (bool, optional):      If True, set the colorbar for the diff to be symmetrical.
                                   Default to True
        outputdir (str, optional): Output directory. Defaults to ".".
        filename (str, optional):  Filename. Defaults to 'map.png'.
        title (str, optional):     Title of the figure. Defaults to None.
        loglevel (str, optional):  Log level. Defaults to 'WARNING'.
        **kwargs:                  Keyword arguments for plot_single_map.
                                   Check the docstring of plot_single_map.

    Raise:
        ValueError: If data or data_ref is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map_diff')

    if isinstance(data_ref, xr.DataArray) is False or isinstance(data, xr.DataArray) is False:
        raise ValueError("data and data_ref must be a DataArray")

    contour = kwargs.get('contour', True)

    # Plot the difference
    diff_map = data - data_ref

    fig, ax = plot_single_map(diff_map, return_fig=True,
                              contour=contour, sym=sym,
                              save=False, loglevel=loglevel,
                              vmin=vmin_fill, vmax=vmax_fill,
                              **kwargs)

    logger.info("Plotting the map as contour")

    cyclic_lon = kwargs.get('cyclic_lon', True)
    if cyclic_lon:
        logger.info("Adding cyclic longitude to the difference map")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)
            logger.warning("Cyclic longitude can be set to False with the cyclic_lon kwarg")

    # Evaluate vmin and vmax of the contour
    if vmin_contour is None or vmax_contour is None:
        vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[data],
                                                              sym=sym_contour)
    else:
        if sym_contour:
            logger.warning("sym_contour=True, vmin_map and vmax_map given will be ignored")
            vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[data],
                                                                  sym=sym_contour)

    logger.debug("Setting contour vmin to %s, vmax to %s", vmin_contour, vmax_contour)

    ds = data.plot.contour(ax=ax,
                           transform=ccrs.PlateCarree(),
                           colors='k', levels=10,
                           linewidths=0.5,
                           vmin=vmin_contour, vmax=vmax_contour)

    ax.clabel(ds, fmt='%1.1f', fontsize=6, inline=True)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

    if save:
        logger.debug("Saving figure to %s", outputdir)
        create_folder(outputdir, loglevel=loglevel)
        plot_format = kwargs.get('format', 'pdf')
        if filename.endswith('.png') or filename.endswith('.pdf'):
            logger.debug("Format already set in the filename")
        else:
            filename = f"{filename}.{plot_format}"
        logger.debug("Setting filename to %s", filename)

        logger.info("Saving figure as %s/%s", outputdir, filename)
        if contour:
            dpi = kwargs.get('dpi', 300)
        else:
            dpi = kwargs.get('dpi', 100)
            if dpi == 100:
                logger.info("Setting dpi to 100 by default, use dpi kwarg to change it")

        fig.savefig('{}/{}'.format(outputdir, filename),
                    dpi=dpi, bbox_inches='tight')

    if display is False:
        logger.debug("Display is set to False, closing figure")
        plt.close(fig)

    return_fig = kwargs.get('return_fig', False)
    if return_fig:
        return fig, ax


def _set_ticks(data: xr.DataArray,
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

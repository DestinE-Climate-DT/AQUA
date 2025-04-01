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

from aqua.logger import log_configure
from aqua.util import add_cyclic_lon, evaluate_colorbar_limits, create_folder
from aqua.util import cbar_get_label, set_map_title
from aqua.util import coord_names, set_ticks, ticks_round
from .styles import ConfigStyle


def plot_single_map(data: xr.DataArray,
                    contour=True, sym=False,
                    proj: ccrs.Projection = ccrs.Robinson(), extent=None,
                    style=None, figsize=(11, 8.5), nlevels=11,
                    vmin=None, vmax=None, cmap='RdBu_r', cbar_label=None,
                    title=None, transform_first=True, cyclic_lon=True,
                    loglevel='WARNING',  **kwargs):
    """
    Plot contour or pcolormesh map of a single variable. By default the contour map is plotted.

    Args:
        data (xr.DataArray):         Data to plot.
        contour (bool, optional):    If True, plot a contour map, otherwise a pcolormesh. Defaults to True.
        sym (bool, optional):        If True, set the colorbar to be symmetrical. Defaults to False.
        proj (cartopy.crs.Projection, optional): Projection to use. Defaults to PlateCarree.
        extent (list, optional):     Extent of the map to limit the projection. Defaults to None.
        style (str, optional):       Style to use. Defaults to None (aqua style).
        figsize (tuple, optional):   Figure size. Defaults to (11, 8.5).
        nlevels (int, optional):     Number of levels for the contour map. Defaults to 11.
        vmin (float, optional):      Minimum value for the colorbar. Defaults to None.
        vmax (float, optional):      Maximum value for the colorbar.
                                     Defaults to None.
        cmap (str, optional):        Colormap. Defaults to 'RdBu_r'.
        cbar_label (str, optional):  Colorbar label. Defaults to None.
        title (str, optional):       Title of the figure. Defaults to None.
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to True.
        cyclic_lon (bool, optional): If True, add cyclic longitude. Defaults to True.
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
    ConfigStyle(style=style)

    # We load in memory the data, to speed up the plotting, Dask is slow with matplotlib
    logger.debug("Loading data in memory")
    data = data.load(keep_attrs=True)

    if cyclic_lon:
        logger.debug("Adding cyclic longitude")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)

    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection=proj)

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

    # Plot the data
    if contour:
        cs = data.plot.contourf(ax=ax,
                                transform=ccrs.PlateCarree(),
                                cmap=cmap,
                                vmin=vmin, vmax=vmax,
                                levels=levels,
                                extend='both',
                                transform_first=transform_first,
                                add_colorbar=False)
    else:
        cs = data.plot.pcolormesh(ax=ax,
                                  transform=ccrs.PlateCarree(),
                                  cmap=cmap,
                                  vmin=vmin, vmax=vmax,
                                  add_colorbar=False)

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

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.1, 0.15, 0.8, 0.02])

    cbar_label = cbar_get_label(data, cbar_label=kwargs.get('cbar_label', None), loglevel=loglevel)
    logger.debug("Setting colorbar label to %s", cbar_label)

    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal', label=cbar_label)

    # Make tick of colorbar simmetric if sym=True
    cbar_ticks_rounding = kwargs.get('cbar_ticks_rounding', None)
    if sym:
        logger.debug("Setting colorbar ticks to be symmetrical")
        cbar_ticks = np.linspace(-vmax, vmax, nlevels + 1)
    else:
        cbar_ticks = np.linspace(vmin, vmax, nlevels + 1)
    if cbar_ticks_rounding is not None:
        logger.debug("Setting colorbar ticks rounding to %s", cbar_ticks_rounding)
        cbar_ticks = ticks_round(cbar_ticks, cbar_ticks_rounding)
    cbar.set_ticks(cbar_ticks)
    cbar.ax.ticklabel_format(style='sci', axis='x', scilimits=(-3, 3))

    # Set x-y labels
    ax.set_xlabel('Longitude [deg]')
    ax.set_ylabel('Latitude [deg]')

    # Set title
    title = set_map_title(data, title=kwargs.get('title', None), loglevel=loglevel)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

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
                                   return_fig will be used to return the figure and axes.

    Raise:
        ValueError: If data or data_ref is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map_diff')

    if isinstance(data_ref, xr.DataArray) is False or isinstance(data, xr.DataArray) is False:
        raise ValueError("data and data_ref must be a DataArray")

    contour = kwargs.get('contour', True)

    # Plot the difference
    diff_map = data - data_ref

    if np.allclose(diff_map, 0):
        logger.warning("The difference map is zero or constant, skipping contour plot.")
        contour = False  # Disable contour

    return_main_fig = kwargs.get('return_fig', False)

    for key in ['return_fig', 'contour']:
        kwargs.pop(key, None) 

    fig, ax = plot_single_map(diff_map, return_fig=True,
                              contour=contour,  # Disable contour for the color map
                              sym=sym,
                              save=False, loglevel=loglevel,
                              vmin=vmin_fill, vmax=vmax_fill,
                              **kwargs)

    logger.info("Plotting the map")

    cyclic_lon = kwargs.get('cyclic_lon', True)
    if cyclic_lon:
        logger.info("Adding cyclic longitude to the difference map")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)
            logger.warning("Cyclic longitude can be set to False with the cyclic_lon kwarg")

    if contour:
        logger.info("Plotting the map as contour")

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

        fmt = {level: f"{level:.1e}" if (abs(level) < 0.1 or abs(level) > 1000) else f"{level:.1f}" for level in ds.levels}
        ax.clabel(ds, fmt=fmt, fontsize=6, inline=True)

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

    if return_main_fig:
        return fig, ax

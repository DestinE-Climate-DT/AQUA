"""
Module to plot multiple maps

"""
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from aqua.core.logger import log_configure
from aqua.core.util import plot_box, evaluate_colorbar_limits, cbar_get_label
from .single_map import plot_single_map, plot_single_map_diff
from .styles import ConfigStyle


def plot_maps(maps: list,
              contour: bool = True, sym: bool = False,
              proj: ccrs.Projection = ccrs.Robinson(), extent: list = None,
              style=None, figsize: tuple = None,
              vmin: float = None, vmax: float = None, nlevels: int = 11,
              title: str = None, title_size: int = 16, titles: list = None, titles_size: int = None, 
              cmap='RdBu_r', cbar_label: str = None,
              transform_first=False, cyclic_lon=True,
              return_fig=False, loglevel='WARNING', **kwargs):
    """
    Plot multiple maps.
    This is supposed to be used for maps to be compared together.
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):          list of xarray.DataArray objects
        contour (bool,opt):   If True, plot a contour map, otherwise a pcolormesh. Defaults to True.
        sym (bool,opt):       symetric colorbar, default is False
        proj (cartopy.crs.Projection,opt): projection, default is ccrs.Robinson()
        extent (list,opt):    extent of the map, default is None
        style (str,opt):      style for the plot, default is the AQUA style
        figsize (tuple,opt):  figure size, default is (6,6) for each map. Here the full figure size is set.
        vmin (float,opt):     minimum value for the colorbar, default is None
        vmax (float,opt):     maximum value for the colorbar, default is None
        nlevels (int,opt):    number of levels for the colorbar, default is 11
        title (str,opt):      super title for the figure
        title_size (int,opt):  size of the super title, default is 16
        titles (list,opt):    list of titles for the maps
        titles_size (int,opt): size of the titles, default is None
        cmap (str,opt):       colormap, default is 'RdBu_r'
        cbar_label (str,opt): colorbar label
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to False.
        cyclic_lon (bool,opt): add cyclic longitude, default is True
        return_fig (bool,opt): return the figure, default is False
        loglevel (str,opt):   log level, default is 'WARNING'
        **kwargs:             Keyword arguments for plot_single_map

    Raises:
        ValueError: if nothing to plot, i.e. maps is None or not a list of xarray.DataArray

    Return:
        fig     if more manipulations on the figure are needed, if return_fig=True
    """
    logger = log_configure(loglevel, 'plot_maps')
    ConfigStyle(style=style, loglevel=loglevel)
    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps):
        raise ValueError('Maps should be a list of xarray.DataArray')
    else:
        logger.debug('Loading maps')
        maps = [data_map.load(keep_attrs=True) for data_map in maps]

    # Generate the figure
    nrows, ncols = plot_box(len(maps))
    figsize = figsize if figsize is not None else (ncols*6, nrows*5 + 1)
    logger.debug('Creating a %d x %d grid with figsize %s', nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)

    # Evaluate min and max values for the common colorbar
    if vmin is None or vmax is None or sym:
        vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)

    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)

    for i in range(len(maps)):
        logger.debug("Plotting map %d", i)
        fig, ax = plot_single_map(data=maps[i], contour=contour,
                                  proj=proj, extent=extent,
                                  vmin=vmin, vmax=vmax, nlevels=nlevels,
                                  title=titles[i] if titles is not None else None,
                                  title_size=titles_size,
                                  cmap=cmap, cbar=False,
                                  transform_first=transform_first, return_fig=True,
                                  cyclic_lon=cyclic_lon, fig=fig, loglevel=loglevel,
                                  ax_pos=(nrows, ncols, i+1), **kwargs)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.87, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.2)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.03])

    cbar_label = cbar_get_label(data=maps[0], cbar_label=cbar_label, loglevel=loglevel)
    logger.debug('Setting colorbar label to %s', cbar_label)

    mappable = ax.collections[0]

    # Add the colorbar
    cbar = fig.colorbar(mappable, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make the colorbar ticks symmetrical if sym=True
    if sym:
        logger.debug('Setting colorbar ticks to be symmetrical')
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
    else:
        cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

    cbar.ax.ticklabel_format(style='sci', axis='x', scilimits=(-3, 3))

    # Add a super title
    if title:
        logger.debug('Setting super title to %s', title)
        fig.suptitle(title, fontsize=title_size)

    if return_fig:
        return fig


def plot_maps_diff(maps: list,
                   maps_ref: list,
                   contour: bool = True,
                   sym: bool = True, sym_contour: bool = False,
                   proj: ccrs.Projection = ccrs.Robinson(), extent: list = None,
                   style=None, figsize=None,
                   vmin_fill: float = None, vmax_fill: float = None,
                   vmin_contour: float = None, vmax_contour: float = None,
                   nlevels: int = 11, title: str = None, titles: list = None,
                   titles_size: int = None,
                   cmap='RdBu_r', cbar_label: str = None,
                   transform_first=False, cyclic_lon=True,
                   return_fig=False, loglevel='WARNING', **kwargs):
    """
    Plot the difference of multiple maps. This is supposed to be used for maps to be compared together.
    Two lists of xarray.DataArray objects are expected
    and the difference (maps[i] - maps_ref[i]) is plotted for each pair.

    Args:
        maps (list):          list of xarray.DataArray objects
        maps_ref (list):      list of xarray.DataArray reference objects
        contour (bool,opt):   If True, plot a contour map, otherwise a pcolormesh. Defaults to True.
        sym (bool,opt):       symetric colorbar, default is False
        sym_contour (bool,opt): symetric colorbar for the contour, default is False
        proj (cartopy.crs.Projection,opt): projection, default is ccrs.Robinson()
        extent (list,opt):    extent of the map, default is None
        style (str,opt):      style for the plot, default is the AQUA style
        figsize (tuple,opt):  figure size, default is (6,6) for each map. Here the full figure size is set.
        vmin_fill (float,opt):     minimum value for the colorbar of the filled map, default is None
        vmax_fill (float,opt):     maximum value for the colorbar of the filled map, default is None
        vmin_contour (float,opt): minimum value for the colorbar of the contour map, default is None
        vmax_contour (float,opt): maximum value for the colorbar of the contour map, default is None
        nlevels (int,opt):    number of levels for the colorbar, default is 11
        title (str,opt):      super title for the figure
        titles (list,opt):    list of titles for the maps
        titles_size (int,opt): size of the titles, default is None
        cmap (str,opt):       colormap, default is 'RdBu_r'
        cbar_label (str,opt): colorbar label
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to False.
        cyclic_lon (bool,opt): add cyclic longitude, default is True
        return_fig (bool,opt): return the figure, default is False
        loglevel (str,opt):   log level, default is 'WARNING'
        **kwargs:             Keyword arguments for plot_single_map

    Raises:
        ValueError: if nothing to plot, i.e. maps, ref_maps is None or not a list of xarray.DataArray

    Return:
        fig     if more manipulations on the figure are needed, if return_fig=True
    """
    logger = log_configure(loglevel, 'plot_maps_diff')
    ConfigStyle(style=style, loglevel=loglevel)

    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps)\
            or maps_ref is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps_ref):
        raise ValueError('Maps and reference maps should be lists of xarray.DataArray')
    else:
        logger.debug('Loading maps and reference maps')
        maps = [data_map.load(keep_attrs=True) for data_map in maps]
        maps_ref = [data_map.load(keep_attrs=True) for data_map in maps_ref]

    # Calculate differences
    diffs = [data_map - data_map_ref for data_map, data_map_ref in zip(maps, maps_ref)]

    # Generate the figure
    nrows, ncols = plot_box(len(diffs))
    figsize = figsize if figsize is not None else (ncols*6, nrows*5 + 1)
    logger.debug('Creating a %d x %d grid with figsize %s', nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)

    # Evaluate min and max values for the common colorbar
    if vmin_fill is None or vmax_fill is None or sym:
        vmin_fill, vmax_fill = evaluate_colorbar_limits(maps=diffs, sym=sym)
    logger.debug("Setting vmin_fill to %s, vmax_fill to %s", vmin_fill, vmax_fill)

    if vmin_contour is None or vmax_contour is None or sym_contour:
        vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=maps, sym=sym_contour)
    logger.debug("Setting vmin_contour to %s, vmax_contour to %s", vmin_contour, vmax_contour)

    for i in range(len(diffs)):
        logger.debug("Plotting map %d", i)
        fig, ax = plot_single_map_diff(data=maps[i], data_ref=maps_ref[i], contour=contour,
                                       proj=proj, extent=extent, style=style,
                                       vmin_fill=vmin_fill, vmax_fill=vmax_fill, nlevels=nlevels,
                                       title=titles[i] if titles is not None else None,
                                       title_size=titles_size,
                                       cmap=cmap, cbar=False,
                                       sym=False, sym_contour=False,
                                       transform_first=transform_first, return_fig=True,
                                       cyclic_lon=cyclic_lon, fig=fig, loglevel=loglevel,
                                       ax_pos=(nrows, ncols, i+1), **kwargs)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.2)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.03])

    cbar_label = cbar_get_label(data=diffs[0], cbar_label=cbar_label, loglevel=loglevel)
    logger.debug('Setting colorbar label to %s', cbar_label)

    mappable = ax.collections[0]

    # Add the colorbar
    cbar = fig.colorbar(mappable, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make the colorbar ticks symmetrical if sym=True
    if sym:
        logger.debug('Setting colorbar ticks to be symmetrical')
        cbar.set_ticks(np.linspace(-vmax_fill, vmax_fill, nlevels + 1))
    else:
        cbar.set_ticks(np.linspace(vmin_fill, vmax_fill, nlevels + 1))

    cbar.ax.ticklabel_format(style='sci', axis='x', scilimits=(-3, 3))

    # Add a super title
    if title:
        logger.debug('Setting super title to %s', title)
        fig.suptitle(title, fontsize=16)

    if return_fig:
        return fig

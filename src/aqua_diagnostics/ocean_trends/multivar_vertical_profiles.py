"""
Module to plot multiple maps

"""
from aqua.graphics import plot_vertical_profile
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from aqua.logger import log_configure
from aqua.util import plot_box, evaluate_colorbar_limits, cbar_get_label
from aqua.graphics.styles import ConfigStyle
from mpl_toolkits.axes_grid1 import make_axes_locatable

def plot_multivars_vertical_profile(
    maps: list[xr.DataArray],
    sym: bool = False,
    style=None,
    figsize: tuple = None,
    ncols: int = None,
    nrows: int = None,
    vmin: float = None,
    vmax: float = None,
    nlevels: int = 12,
    title: str = None,
    titles: list = None,
    cmap: str = "RdBu_r",
    cbar_labels: list = None,
    return_fig: bool = False,
    ytext: list = None,
    loglevel: str = "WARNING",
    **kwargs,
):
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
        titles (list,opt):    list of titles for the maps
        cmap (str,opt):       colormap, default is 'RdBu_r'
        cbar_labels (list,opt): colorbar labels
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
    logger = log_configure(loglevel, "plot_maps")
    ConfigStyle(style=style, loglevel=loglevel)

    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps):
        raise ValueError("Maps should be a list of xarray.DataArray")
    else:
        logger.debug("Loading maps")
        maps = [data_map.load(keep_attrs=True) for data_map in maps]

    # Generate the figure, if the number of rows and columns is not provided,
    # try to make a square figure with a reasonable aspect ratio
    if not nrows and not ncols:
        nrows, ncols = plot_box(len(maps))
    figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
    logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(
        bottom=0.25, top=0.9, left=0.05, right=0.95, wspace=0.3, hspace=0.8
    )
    
    for i in range(len(maps)):
        vmin, vmax = evaluate_colorbar_limits(maps=maps[i], sym=sym)

        logger.debug("Plotting map %d", i)
        fig, ax = plot_vertical_profile(
            data=maps[i],
            lev_name='level',
            vmin=vmin,
            vmax=vmax,
            nlevels=nlevels,
            title=titles[i] if titles is not None else None,
            grid=False,
            add_land=True,
            cbar=False,
            return_fig=True,
            fig=fig,
            loglevel=loglevel,
            ax_pos=(nrows, ncols, i + 1),
            **kwargs,
        )
        if ytext:
            logger.debug("Adding text in the plot: %s", ytext[i])
            ax.text(-0.3, 0.33, ytext[i], fontsize=15, color='dimgray', rotation=90, transform=ax.transAxes, ha='center')

        # Retrieve last plotted object for colorbar (QuadMesh or ContourSet)
        if ax.collections:
            mappable = ax.collections[-1]
        elif ax.images:
            mappable = ax.images[-1]
        else:
            logger.warning("No mappable object found for subplot %d", i)
            continue
        
        # Update mappable normalization and cmap
        mappable.set_norm(plt.Normalize(vmin=vmin, vmax=vmax))
        mappable.set_cmap(cmap)

        # Attach colorbar
        divider = make_axes_locatable(ax)
        cax = divider.append_axes("right", size="5%", pad=0.15, axes_class=plt.Axes)
        cbar = fig.colorbar(mappable, cax=cax, orientation="vertical")
        if cbar_labels and i < len(cbar_labels):
            cbar.set_label(cbar_labels[i], fontsize=12)

    # Add a super title
    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=ncols * 12, y=1.1)

    if return_fig:
        return fig

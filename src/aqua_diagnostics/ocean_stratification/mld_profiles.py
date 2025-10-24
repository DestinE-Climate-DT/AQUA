"""
Module to plot multiple maps

"""
from typing import Optional, Tuple
import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from aqua.logger import log_configure
from aqua.util import (
    plot_box,
    evaluate_colorbar_limits,
    cbar_get_label,
    generate_colorbar_ticks,
)
from aqua import plot_single_map
from aqua.graphics.styles import ConfigStyle
from mpl_toolkits.axes_grid1 import make_axes_locatable


def plot_maps(
    maps: list,
    contour: bool = True,
    sym: bool = False,
    proj: ccrs.Projection = ccrs.PlateCarree(),
    extent: list = None,
    style=None,
    figsize: tuple = None,
    ncols: int = None,
    nrows: int = None,
    vmin: float = None,
    vmax: float = None,
    nlevels: int = 11,
    title: str = None,
    titles: list = None,
    cmap="RdBu_r",
    cbar_number: str = "single",
    cbar_label: str = None,
    transform_first=False,
    cyclic_lon=True,
    return_fig=False,
    ytext=None,
    loglevel="WARNING",
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
    logger = log_configure(loglevel, "plot_maps")
    ConfigStyle(style=style, loglevel=loglevel)
    if maps is None or any(not isinstance(data_map, xr.DataArray) for data_map in maps):
        raise ValueError("Maps should be a list of xarray.DataArray")
    else:
        logger.debug("Loading maps")
        maps = [data_map.load(keep_attrs=True) for data_map in maps]

    logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols, figsize)

    # Generate the figure
    if not nrows and not ncols:
        nrows, ncols = plot_box(len(maps))
    figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
    logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)

    if cbar_number == "single":
        # Evaluate min and max values for the common colorbar
        if vmin is None or vmax is None or sym:
            vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)

    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)

    if cbar_number == "single":
        cbar = True
    if cbar_number == "separate":
        cbar = False

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(
        bottom=0.25, top=0.9, left=0.05, right=0.95, wspace=0.1, hspace=0.5
    )

    for i in range(len(maps)):
        if cbar_number == "separate":
            vmin, vmax = evaluate_colorbar_limits(maps=maps[i], sym=sym)

        logger.debug("Plotting map %d", i)
        fig, ax = plot_single_map(
            data=maps[i],
            contour=contour,
            proj=proj,
            extent=extent,
            vmin=vmin,
            vmax=vmax,
            nlevels=nlevels,
            title=titles[i] if titles is not None else None,
            cmap=cmap,
            cbar=False,
            transform_first=transform_first,
            add_land=True,
            return_fig=True,
            cyclic_lon=cyclic_lon,
            fig=fig,
            loglevel=loglevel,
            ax_pos=(nrows, ncols, i + 1),
            ticks_rounding=0,
            **kwargs,
        )
        ax.set_facecolor("lightgray")

        if ytext:
            logger.debug("Adding text in the plot: %s", ytext[i])
            ax.text(
                -0.3,
                0.33,
                ytext[i],
                fontsize=15,
                color="dimgray",
                rotation=90,
                transform=ax.transAxes,
                ha="center",
            )
        if cbar_number == "separate":
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

            cbar_ticks_rounding = kwargs.get("cbar_ticks_rounding", None)
            cbar_ticks = generate_colorbar_ticks(
                vmin=vmin,
                vmax=vmax,
                sym=sym,
                nlevels=nlevels,
                ticks_rounding=cbar_ticks_rounding,
                loglevel=loglevel,
            )
            cbar.set_ticks(cbar_ticks)
    if cbar_number == "single":

        # Add a colorbar axis at the bottom of the graph
        cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.03])

        cbar_label = cbar_get_label(
            data=maps[0], cbar_label=cbar_label, loglevel=loglevel
        )
        logger.debug("Setting colorbar label to %s", cbar_label)

        # Add the colorbar
        mappable = ax.collections[0]
        if cbar == True:
            cbar = fig.colorbar(
                mappable, cax=cbar_ax, orientation="horizontal", label=cbar_label
            )
            # cbar.set_ticks([vmin, vmax])  # Only show min and max
            cbar_ticks_rounding = kwargs.get("cbar_ticks_rounding", None)
            cbar_ticks = generate_colorbar_ticks(
                vmin=vmin,
                vmax=vmax,
                sym=sym,
                nlevels=nlevels,
                ticks_rounding=cbar_ticks_rounding,
                max_ticks=10,
                loglevel=loglevel,
            )
            # cbar.set_ticks([vmin, vmax])
            cbar.set_ticks(cbar_ticks)

        # # Make the colorbar ticks symmetrical if sym=True
        # if sym:
        #     logger.debug("Setting colorbar ticks to be symmetrical")
        #     cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
        # else:
        #     cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

        cbar.ax.ticklabel_format(style="sci", axis="x", scilimits=(-3, 3))

    # Add a super title
    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=ncols * 15, y=1.05)

    if return_fig:
        return fig

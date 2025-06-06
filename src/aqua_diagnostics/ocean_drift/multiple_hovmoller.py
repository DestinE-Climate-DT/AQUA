"""
Module to plot multiple hovemoller data

"""

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua import plot_hovmoller
from aqua.graphics import ConfigStyle
from aqua.logger import log_configure
from aqua.util import cbar_get_label, evaluate_colorbar_limits, plot_box


def plot_multi_hovmoller(
    maps: list,
    contour: bool = True,
    sym: bool = False,
    figsize: tuple = None,
    variables: list = None,
    invert_space_coord=True,
    fig: plt.Figure = None,
    ax: plt.Axes = None,
    style=None,
    vmin: list[float] = None,
    vmax: list[float] = None,
    nlevels: int = 20,
    title: str = None,
    titles: list[str] = None,
    cmap: list[str] = None,
    cbar_label: list[str] = None,
    return_fig=True,
    loglevel="WARNING",
    **kwargs,
):
    logger = log_configure(loglevel, "plot_multi_hovmoller")
    ConfigStyle(style=style, loglevel=loglevel)

    # Generate the figure
    # if maps is None or any(not isinstance(data_map, xr.DataArray)
    #                        for data_map in maps):
    #     nrows, ncols = plot_box(len(maps))
    #     figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
    #     logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols,
    #                 figsize)
    if all(isinstance(data_map, xr.Dataset) for data_map in maps):
        nrows = len(maps)
        ncols = len(variables) #TODO limit ncols to 2 variables
        figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
        logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols,
                    figsize)
        
    fig = plt.figure(figsize=figsize)
    spec = fig.add_gridspec(nrows = nrows, ncols = ncols)

    # # Evaluate min and max values for the common colorbar
    # if vmin is None or vmax is None or sym:
    #     vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)
    for i, var in enumerate(variables):
        for j in range(nrows):
            logger.debug("Plotting map %d", i)
            k = i + j
            ax = fig.add_subplot(spec[j, i])
            fig, ax = plot_hovmoller(
                                    data=maps[j][var],
                                    invert_space_coord=invert_space_coord,
                                    sym=sym,
                                    contour=contour,
                                    box_text=False,
                                    vmin=vmin[j] if vmin is not None else None,
                                    vmax=vmax[j] if vmax is not None else None,
                                    cmap=cmap[j] if cmap is not None else None,
                                    nlevels=nlevels,
                                    cbar_label=cbar_label[j] if cbar_label is not None else None,
                                    cbar_pos=[1, .8 - 0.3*j, 0.023, 0.1],
                                    cbar_orientation='vertical',
                                    title=titles[j] if titles is not None else None,
                                    return_fig=True,
                                    ax = ax,
                                    # ax_pos=(nrows, ncols, k + 1),
                                    fig=fig)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25,
                        top=0.9,
                        left=0.05,
                        right=0.95,
                        wspace=0.1,
                        hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    # cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.03])

    # cbar_label = cbar_get_label(data=maps[0],
    #                             cbar_label=cbar_label,
    #                             loglevel=loglevel)
    logger.debug("Setting colorbar label to %s", cbar_label)

    # mappable = ax.collections[0]

    # Add the colorbar
    # cbar = fig.colorbar(mappable,
    #                     cax=cbar_ax,
    #                     orientation="horizontal",
    #                     label=cbar_label)

    # Make the colorbar ticks symmetrical if sym=True
    # if sym:
    #     logger.debug("Setting colorbar ticks to be symmetrical")
    #     cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
    # else:
    #     cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

    # Add a super title
    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=16)

    if return_fig:
        return fig

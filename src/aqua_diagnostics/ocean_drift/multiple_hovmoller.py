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
    text: list[float] = None,
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

    if all(isinstance(data_map, xr.Dataset) for data_map in maps):
        nrows = len(maps)
        ncols = len(variables) #TODO limit ncols to 2 variables
        figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
        logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols,
                    figsize)
        
    fig = plt.figure(figsize=figsize)
    spec = fig.add_gridspec(nrows=nrows, ncols=ncols)

    for j in range(nrows):
        for i, var in enumerate(variables):
            # if (j, i) != (0, 0):
            #     i_inc = i + 1
            #     k = i_inc + j
            # else:
            #     k = i + j
            # k = j + i * ncols
            k = j * ncols + i
            ax = fig.add_subplot(spec[j, i])
            logger.debug("Creating subplot for variable %s at position (%d, %d)", var, j, i)
            
            fig, ax = plot_hovmoller(
                                    data=maps[j][var],
                                    invert_space_coord=invert_space_coord,
                                    sym=sym,
                                    contour=contour,
                                    box_text=False,
                                    vmin=vmin[k] if vmin is not None else None,
                                    vmax=vmax[k] if vmax is not None else None,
                                    cmap=cmap[k] if cmap is not None else None,
                                    nlevels=nlevels,
                                    text=text[k] if text is not None else None,
                                    cbar_label=cbar_label[j] if cbar_label is not None else None,
                                    # cbar_pos=[1-.5*j, .8 - 0.3*i, 0.023, 0.1],
                                    cbar_orientation='vertical',
                                    title=titles[k] if titles is not None else None,
                                    return_fig=True,
                                    ax=ax,
                                    fig=fig,
                                    loglevel=loglevel)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25,
                        top=0.9,
                        left=0.05,
                        right=0.95,
                        wspace=0.5,
                        hspace=0.5)

    logger.debug("Setting colorbar label to %s", cbar_label)

    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=16)

    if return_fig:
        return fig

"""
Module to plot multiple Hovmoller data.
This function is custom for the Ocean Drift diagnostics in AQUA.
"""
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from mpl_toolkits.axes_grid1 import make_axes_locatable

from aqua.graphics import plot_timeseries
from aqua.graphics import ConfigStyle
from aqua.logger import log_configure
from aqua.util import cbar_get_label, evaluate_colorbar_limits, plot_box


def plot_multi_timeseries(
    maps: list,
    levels: list = None,
    line_plot_colours: list = None,
    figsize: tuple = None,
    variables: list = None,
    fig: plt.Figure = None,
    ax: plt.Axes = None,
    style=None,
    text: list[float] = None,
    title: str = None,
    titles: list[str] = None,
    return_fig=True,
    loglevel="WARNING",
    **kwargs,
):
    """
    Plot multiple time series (e.g., at different levels or for different variables) in a grid layout.

    Args:
        maps (list): List of xarray datasets containing the data to be plotted.
        levels (list, optional): List of levels to plot for each variable (used for labeling and selection).
        line_plot_colours (list, optional): List of colors for the lines in each subplot.
        figsize (tuple, optional): Size of the figure (width, height). If None, it is set automatically.
        variables (list, optional): List of variable names to plot from each dataset.
        fig (plt.Figure, optional): Matplotlib Figure to plot on. If None, a new figure is created.
        ax (plt.Axes, optional): Matplotlib Axes to plot on. If None, new axes are created.
        style (str, optional): Plot style to use (default is AQUA style).
        text (list, optional): List of text annotations for each subplot.
        title (str, optional): Title for the entire figure.
        titles (list of str, optional): List of titles for each subplot. If None, no titles will be set.
        return_fig (bool, optional): If True, return the matplotlib Figure object.
        loglevel (str, optional): Logging level for the function.
        **kwargs: Additional keyword arguments passed to the plotting function.

    Returns:
        matplotlib.figure.Figure or None: The matplotlib Figure object if return_fig is True, otherwise None.
    """
    logger = log_configure(loglevel, "plot_multi_hovmoller")
    ConfigStyle(style=style, loglevel=loglevel)

    if all(isinstance(data_map, xr.Dataset) for data_map in maps):
        nrows = len(maps)
        ncols = len(variables)
        figsize = figsize if figsize is not None else (ncols * 6, nrows * 5 + 1)
        logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)
    spec = fig.add_gridspec(nrows=nrows, ncols=ncols, wspace=0.4, hspace=0.5)

    data_labels = [str(x) for x in levels]

    for j in range(nrows):
        for i, var in enumerate(variables):
            k = j * len(variables) + i
            ax = fig.add_subplot(spec[j, i])
            logger.debug("Creating subplot for variable %s at (%d, %d)", var, j, i)

            fig, ax = plot_timeseries(
                [maps[j][var].sel(level=level) for level in maps[j][var].level],
                data_labels=data_labels if k == 0 else None,
                title=titles[k] if titles else None,
                ax=ax,
                fig=fig,
                loglevel=loglevel,
                colors=line_plot_colours
            )
            ax.set_xticks(ax.get_xticks())
            ax.set_xticklabels(ax.get_xticklabels(), rotation=30)

            if text:
                logger.debug("Adding text in the plot: %s", text)
                ax.text(-0.3, 0.33, text[k], fontsize=15, color='dimgray', rotation=90, transform=ax.transAxes, ha='center')

    # Adjust overall layout
    fig.subplots_adjust(bottom=0.1, top=0.9, left=0.05, right=0.95)
    fig.tight_layout(rect=[0, 0, 1, 0.95])  # Leave space for title

    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=ncols * 10, fontweight='bold')

    if return_fig:
        return fig

import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
from mpl_toolkits.axes_grid1 import make_axes_locatable

from aqua.graphics import ConfigStyle, plot_vertical_lines
from aqua.logger import log_configure
from aqua.util import cbar_get_label, evaluate_colorbar_limits, plot_box


def plot_multi_vertical_lines(
    data_list: list,
    ref_data_list: list,
    nrows: int,
    ncols: int,
    figsize: tuple = None,
    data_label: str = None,
    obs_label: str = None,
    variables: list = None,
    fig: plt.Figure = None,
    ax: plt.Axes = None,
    style=None,
    text: list[float] = None,
    title: str = None,
    return_fig=True,
    loglevel="WARNING",
    **kwargs,
):
    """
    Plot multiple vertical line profiles in a grid layout.

    Args:
        data_list (list): List of xarray datasets containing the data to be plotted.
        ref_data_list (list): List of xarray datasets containing the reference data to be plotted.
        nrows (int): Number of rows in the subplot grid.
        ncols (int): Number of columns in the subplot grid.
        figsize (tuple, optional): Size of the figure (width, height). If None, it is set automatically.
        data_label (str, optional): Label for the main data lines.
        obs_label (str, optional): Label for the reference or observational lines.
        variables (list, optional): List of variable names to plot from each dataset.
        fig (plt.Figure, optional): Matplotlib Figure to plot on. If None, a new figure is created.
        ax (plt.Axes, optional): Matplotlib Axes to plot on. If None, new axes are created.
        style (str, optional): Plot style to use (default is AQUA style).
        text (list, optional): List of text annotations for each subplot.
        title (str, optional): Title for the entire figure.
        return_fig (bool, optional): If True, return the matplotlib Figure object.
        loglevel (str, optional): Logging level for the function.
        **kwargs: Additional keyword arguments passed to the plotting function.

    Returns:
        matplotlib.figure.Figure or None: The matplotlib Figure object if return_fig is True, otherwise None.
    """
    logger = log_configure(loglevel, "plot_multi_hovmoller")
    ConfigStyle(style=style, loglevel=loglevel)

    if all(isinstance(data_map, xr.Dataset) for data_map in data_list):
        nrows = 1  # len(data_list)
        ncols = len(variables)
        figsize = figsize if figsize is not None else (ncols * 5, nrows * 3 + 1)
        logger.debug("Creating a %d x %d grid with figsize %s", nrows, ncols, figsize)

    fig = plt.figure(figsize=figsize)
    spec = fig.add_gridspec(nrows=nrows, ncols=ncols, wspace=0.2, hspace=0.1)

    for j in range(nrows):
        for i, var in enumerate(variables):
            k = j * len(variables) + i
            ax = fig.add_subplot(spec[j, i])
            logger.debug("Creating subplot for variable %s at (%d, %d)", var, j, i)

            fig, ax = plot_vertical_lines(
                data=data_list[j][var],
                ref_data=ref_data_list[j][var] if ref_data_list else None,
                labels=data_label,
                ref_label=obs_label,
                lev_name="level",
                invert_yaxis=True,
                title=variables[i],
                return_fig=True,
                ax=ax,
                fig=fig,
                loglevel=loglevel,
            )
            ax.set_xticks(ax.get_xticks())
            ax.set_xticklabels(ax.get_xticklabels(), rotation=30)

            if text:
                logger.debug("Adding text in the plot: %s", text)
                ax.text(
                    -0.3,
                    0.33,
                    text[k],
                    fontsize=15,
                    color="dimgray",
                    rotation=90,
                    transform=ax.transAxes,
                    ha="center",
                )

    # Adjust overall layout
    fig.subplots_adjust(bottom=0.1, top=0.9, left=0.05, right=0.95)
    fig.tight_layout(rect=[0, 0, 1, 0.95])  # Leave space for title

    if title:
        logger.debug("Setting super title to %s", title)
        fig.suptitle(title, fontsize=ncols * 10, fontweight="bold", y=1.05)

    if return_fig:
        return fig

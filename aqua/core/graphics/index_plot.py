import xarray as xr
import matplotlib.pyplot as plt
from aqua.core.logger import log_configure
from .styles import ConfigStyle


def index_plot(index: xr.DataArray, thresh: float = 0,
               fig: plt.Figure = None, ax: plt.Axes = None,
               style: str = None, figsize: tuple = (11, 8.5),
               title: str = None, ylim: tuple = None,
               ylabel: str = None, label: str = None, loglevel='WARNING'):
    """
    Index plot together with a black line at index=0.
    Values above thresh are filled in red, values below thresh are filled in blue.

    Args:
        index (DataArray):     Index DataArray
        thresh (float,opt):    Threshold for the index, default is 0
        fig (Figure,opt):      Figure object
        ax (Axes,opt):         Axes object
        style (str, optional): Style to use. Defaults to None (aqua style).
        figsize (tuple,opt):   Figure size, default is (11, 8.5)
        title (str,opt):       Title for the plot. Default is None
        ylim (tuple,opt):      y-axis limits. Default is None
        ylabel (str,opt):      y-axis label. Default is None
        label (str,opt):       Label for the plot. Default is None
        loglevel (str,opt):    Loglevel for the logger. Default is 'WARNING'

    Returns:
        fig, ax: Figure and Axes objects
    """
    logger = log_configure(loglevel, 'index_plot')
    ConfigStyle(style=style, loglevel=loglevel)

    logger.debug('Loading data in memory')
    index = index.load()

    if fig is None:
        fig = plt.figure(figsize=figsize)
    if ax is None:
        ax = fig.add_subplot(111)

    if ylim is not None:
        ax.set_ylim(ylim)

    # Plot the index
    ax.fill_between(x=index.time, y1=thresh, y2=index.values, where=index.values >= thresh,
                    alpha=0.6, color='red', interpolate=True)
    ax.fill_between(index.time, y1=-thresh, y2=index.values, where=index.values < -thresh,
                    alpha=0.6, color='blue', interpolate=True)
    index.plot(ax=ax, color='black', alpha=0.8, label=label)

    ax.axhline(y=0, color='black', alpha=0.5)
    ax.grid(True, axis="y", linestyle='-', color='silver', alpha=0.8)

    if title is not None:
        ax.set_title(title)

    # Set the ylabel
    if ylabel is not None:
        ax.set_ylabel(ylabel)
    else:
        ax.set_ylabel('Index')

    if label is not None:
        ax.legend()

    return fig, ax


def indexes_plot(indexes: list, thresh: float = 0,
                 style=None, figsize: tuple = None,
                 titles: list = None, labels: list = None,
                 suptitle: str = None,
                 ylabel: str = None, loglevel='WARNING'):
    """
    Use the plot_index function to plot two indexes in two
    subplots.

    Args:
        indexes (list):         List of DataArray objects
        thresh (float,opt):     Threshold for the index, default is 0
        style (str, optional):  Style to use. Defaults to None (aqua style).
        figsize (tuple,opt):    Figure size, default is (5.5*n_indexes, 8.5)
        titles (list,opt):      List of strings for the titles of the plots. Default is None
        labels (list,opt):      List of strings for the labels of the plots. Default is None
        suptitle (str,opt):     Title for the figure. Default is None
        ylabel (str,opt):       y-axis label. Default is None
        loglevel (str,opt):     Loglevel for the logger. Default is 'WARNING'

    Returns:
        fig, axs: Figure and Axes objects
    """
    figsize = (8.5, 5.5 * len(indexes)) if figsize is None else figsize
    ConfigStyle(style=style, loglevel=loglevel) 

    fig, axs = plt.subplots(nrows=len(indexes), ncols=1, figsize=figsize, sharex=False)

    # Evaluating a common ylim:
    ymin = min(index.min().values for index in indexes)
    ymax = max(index.max().values for index in indexes)
    ylim = [ymin, ymax]

    if isinstance(axs, plt.Axes):
        axs = [axs]

    for i, index in enumerate(indexes):
        fig, axs[i] = index_plot(index, thresh=thresh,
                                 fig=fig, ax=axs[i],
                                 style=style, figsize=figsize,
                                 title=titles[i] if titles is not None else None,
                                 label=labels[i] if labels is not None else None,
                                 ylim=ylim, ylabel=ylabel,
                                 loglevel=loglevel)

    if suptitle is not None:
        fig.suptitle(suptitle)

    fig.tight_layout()

    return fig, axs

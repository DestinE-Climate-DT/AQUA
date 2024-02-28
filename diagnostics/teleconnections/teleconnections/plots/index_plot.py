"""
This module contains simple function for index plotting.
"""

import os

import xarray as xr
import matplotlib.pyplot as plt
from aqua.logger import log_configure


def index_plot(indx, save=False, outputdir='./', filename='index.pdf',
               step=False, loglevel='WARNING', **kwargs):
    """
    Index plot together with a black line at indx=0.
    Values above 0 are filled in red, values below 0 are filled in blue.

    Args:
        indx (DataArray): Index DataArray
        save (bool,opt):        enable or disable saving the plot,
                                default is False
        outputdir (str,opt):    directory to save the plot,
                                default is './'
        filename (str,opt):     filename for the plot
                                default is 'index.png'
        step (bool,opt):        enable or disable step plot,
                                default is False
        loglevel (str,opt):     log level for the logger,
                                default is 'WARNING'
        **kwargs:               additional arguments

    Kwargs:
        - figsize (tuple,opt):    figure size, default is (11, 8.5)
        - title (str,opt):        title for the plot
        - ylabel (str,opt):       ylabel for the plot
        - ylim (tuple,opt):       y-axis limits
        - fig (Figure,opt):       Figure object
        - ax (Axes,opt):          Axes object

    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    """
    logger = log_configure(loglevel, 'Index plot')

    logger.debug('Loading data in memory')
    indx = indx.load()

    # Generate the figure
    figsize = kwargs.get('figsize', (11, 8.5))
    fig = kwargs.get('fig', None)
    ax = kwargs.get('ax', None)
    if fig is None and ax is None:
        fig, ax = plt.subplots(figsize=figsize)
        logger.debug("Figure and Axes created")
    elif fig is not None and ax is not None:
        logger.debug("Figure and Axes provided")
    else:
        raise ValueError('Both fig and ax must be provided or none of them')

    ylim = kwargs.get('ylim', None)
    if ylim is not None:
        ax.set_ylim(ylim)
        logger.debug("ylim set to %s", ylim)

    # Plot the index
    if step:
        logger.warning('Step plot has some issues with fill_between')
        logger.warning('Steps are not filled if a change in sign occurs')

        ax.fill_between(indx.time, indx.values, where=indx.values >= 0,
                        step="pre", alpha=0.6, color='red')
        ax.fill_between(indx.time, indx.values, where=indx.values < 0,
                        step="pre", alpha=0.6, color='blue')
        indx.plot.step(ax=ax, color='black', alpha=0.8)
    else:
        ax.fill_between(indx.time, indx.values, where=indx.values >= 0,
                        alpha=0.6, color='red', interpolate=True)
        ax.fill_between(indx.time, indx.values, where=indx.values < 0,
                        alpha=0.6, color='blue', interpolate=True)
        indx.plot(ax=ax, color='black', alpha=0.8)

    ax.hlines(y=0, xmin=min(indx['time']), xmax=max(indx['time']),
              color='black')

    title = kwargs.get('title')

    if title is not None:
        ax.set_title(title)
        logger.debug("Title set to %s", title)

    # Set the ylabel
    ylabel = kwargs.get('ylabel')
    if ylabel is not None:
        ax.set_ylabel(ylabel)
        logger.debug("ylabel set to %s", ylabel)
    else:
        ax.set_ylabel('Index')

    # Save the figure
    if save:
        filepath = os.path.join(outputdir, filename)
        fig.savefig(filepath)
        logger.debug("Fig saved in %s", filepath)

    return fig, ax


def indexes_plot(indx1: xr.DataArray, indx2: xr.DataArray,
                 titles: list = None,
                 title: str = None,
                 save=False, outputdir='./', filename='indexes.pdf',
                 loglevel='WARNING', **kwargs):
    """
    Use the plot_index function to plot two indexes in two
    subplots.

    Args:
        - indx1 (DataArray):    first index
        - indx2 (DataArray):    second index
        - titles (list,opt):    list of titles for the plots
        - title (str,opt):      title for the figure
        - save (bool,opt):      enable or disable saving the plot,
                                default is False
        - outputdir (str,opt):  directory to save the plot,
                                default is './'
        - filename (str,opt):   filename for the plot
                                default is 'indexes.png'
        - loglevel (str,opt):   log level for the logger,

    Kwargs are the same as for the index_plot function.
    """
    logger = log_configure(loglevel, 'Indexes plot')

    figsize = kwargs.get('figsize', (11, 8.5))
    fig, axs = plt.subplots(2, 1, figsize=figsize)

    if titles is not None:
        if len(titles) != 2:
            logger.error('Titles must be a list of two strings')
            title1 = None
            title2 = None
        title1 = titles[0]
        title2 = titles[1]
    else:
        title1 = None
        title2 = None

    # Evaluating a common ylim:
    ymin = min(indx1.min().values, indx2.min().values)
    ymax = max(indx1.max().values, indx2.max().values)
    ylim = [ymin, ymax]

    logger.debug('Plotting the first index')
    fig, axs[0] = index_plot(indx1, loglevel=loglevel,
                             fig=fig, ax=axs[0],
                             title=title1, ylim=ylim,
                             save=False, **kwargs)
    logger.debug('Plotting the second index')
    fig, axs[1] = index_plot(indx2, loglevel=loglevel,
                             fig=fig, ax=axs[1],
                             title=title2, ylim=ylim,
                             save=False, **kwargs)

    if title is not None:
        fig.suptitle(title)

    fig.tight_layout()

    if save:
        filepath = os.path.join(outputdir, filename)
        fig.savefig(filepath)
        logger.info("Fig saved in %s", filepath)

    return fig, axs

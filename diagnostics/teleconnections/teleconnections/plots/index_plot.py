"""
This module contains simple function for index plotting.
"""

import os

import matplotlib.pyplot as plt
from aqua.logger import log_configure


def index_plot(indx, save=False, outputdir='./', filename='index.png',
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

    Returns:
        fig (Figure):           Figure object
        ax (Axes):              Axes object
    """
    logger = log_configure(loglevel, 'Index plot')

    logger.debug('Loading data in memory')
    indx = indx.load()

    # Generate the figure
    figsize = kwargs.get('figsize', (11, 8.5))
    fig, ax = plt.subplots(figsize=figsize)

    ylim = kwargs.get('ylim', None)
    if ylim is not None:
        ax.set_ylim(ylim)
        logger.debug("ylim set to %s", ylim)

    # Plot the index
    if step:
        logger.warning('Step plot has some issues with fill_between')
        logger.warning('Steps are not filled if a change in sign occurs')

        plt.fill_between(indx.time, indx.values, where=indx.values >= 0,
                         step="pre", alpha=0.6, color='red')
        plt.fill_between(indx.time, indx.values, where=indx.values < 0,
                         step="pre", alpha=0.6, color='blue')
        indx.plot.step(ax=ax, color='black', alpha=0.8)
    else:
        plt.fill_between(indx.time, indx.values, where=indx.values >= 0,
                         alpha=0.6, color='red', interpolate=True)
        plt.fill_between(indx.time, indx.values, where=indx.values < 0,
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

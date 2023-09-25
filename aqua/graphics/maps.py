"""Common AQUA maps functions"""
import os

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.logger import log_configure
from aqua.util import add_cyclic_lon, evaluate_colorbar_limits


def single_map_plot(map: xr.DataArray, save=False, model=None, exp=None,
                    figsize=(11, 8.5), nlevels=12, title=None,
                    cbar_label=None, outputdir='.', filename='maps.png',
                    sym=False, loglevel='WARNING'):
    """
    Plot a single map (regression, correlation, etc.)
    An xarray.DataArray objects is expected
    and a map is plotted

    Args:
        map (xarray.DataArray): xarray.DataArray object
        save (bool, opt):       save the figure
        model (str,opt):        model name
        exp (str,opt):          experiment name
        figsize (tuple,opt):    figure size, default is (11, 8.5)
        nlevels (int,opt):      number of levels for the colorbar, default is 12
        title (str,opt):        title for the figure
        cb_label (str,opt):     label for the colorbar
        outputdir (str,opt):    output directory for the figure, default is '.' (current directory)
        filename (str,opt):     filename for the figure, default is 'maps.png'
        sym (bool,opt):         symmetrical colorbar, default is False
        loglevel (str,opt):     log level for the logger, default is 'WARNING'
    """
    logger = log_configure(loglevel, 'Single map')

    # Add cyclic longitude
    map = add_cyclic_lon(map)

    vmin, vmax = evaluate_colorbar_limits(maps=[map], sym=sym)

    # Generate the figure)
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()},
                           figsize=figsize)

    # Plot the map
    try:
        logger.info('Plotting model {} experiment {}'.format(model, exp))
    except ValueError:
        logger.info('Plotting map')

    # Contour plot
    cs = map.plot.contourf(ax=ax, transform=ccrs.PlateCarree(),
                           cmap='RdBu_r', levels=nlevels,
                           add_colorbar=False, add_labels=False,
                           extend='both', vmin=vmin, vmax=vmax)

    # Title
    if title is not None:
        ax.set_title(title)
    else:
        try:
            ax.set_title('{} {}'.format(model, exp))
        except ValueError:
            logger.warning('No title for map, set it with the title argument')

    # Coastlines
    ax.coastlines()

    # Longitude labels
    ax.set_xticks(np.arange(-180, 181, 60), crs=ccrs.PlateCarree())
    lon_formatter = cticker.LongitudeFormatter()
    ax.xaxis.set_major_formatter(lon_formatter)

    # Latitude labels
    ax.set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
    lat_formatter = cticker.LatitudeFormatter()
    ax.yaxis.set_major_formatter(lat_formatter)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    # Add the colorbar
    if cbar_label is not None:
        fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                     label=cbar_label)
    else:
        try:
            fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                         label=map.short_name)
        except AttributeError:
            fig.colorbar(cs, cax=cbar_ax, orientation='horizontal')

    # Save the figure
    if save is True:
        # check the outputdir exists and create it if necessary
        if not os.path.exists(outputdir):
            logger.info('Creating output directory {}'.format(outputdir))
            os.makedirs(outputdir)
        if filename is None:
            try:
                filename = model + '_' + exp + '.pdf'
            except ValueError:
                logger.warning('No filename for map, you can set it with the filename argument')
                filename = 'map.pdf'

        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')

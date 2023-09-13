"""
Module to plot maps (regression, correlation, etc.)

Functions:
    maps_plot:          plot multiple maps
    single_map_plot:    plot a single map
    maps_diffs_plot:    plot multiple maps and as contours
                        the differences previously computed
"""
import os

import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np

from aqua.logger import log_configure
from .plot_utils import minmax_maps, plot_box, add_cyclic_lon


def maps_plot(maps=None, models=None, exps=None,
              titles=None, save=False, **kwargs):
    """Plot maps (regression, correlation, etc.)
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):        list of xarray.DataArray objects
        models (list):      list of models
        exps (list):        list of experiments
        titles (list, opt): list of titles for each map
                            overrides models and exps standard titles
        save (bool, opt):   save the figure

    Kwargs:
        - loglevel (str,opt):   log level for the logger, default is 'WARNING'
        - figsize (tuple,opt):  figure size, default is (11, 8.5)
        - nlevels (int,opt):    number of levels for the colorbar, default is 11
        - cbar_label (str,opt): label for the colorbar
        - title (str,opt):      title for the figure (suptitle)
    """
    loglevel = kwargs.get('loglevel', 'WARNING')
    logger = log_configure(loglevel, 'Multiple maps')

    if maps is None:
        raise ValueError('Nothing to plot')

    if models is None and titles is None:
        logger.warning('No titles provided')
    if exps is None and titles is None:
        logger.warning('No titles provided')

    # Generate the figure
    nrows, ncols = plot_box(len(maps))
    figsize = kwargs.get('figsize', (11, 8.5))

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = minmax_maps(maps)
    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    # Drop unused axes
    for i in range(len(axs)):
        if i >= len(maps):
            axs[i].axis('off')
            logger.debug('Dropping unused axes {}'.format(i))

    # Set the number of levels for the colorbar
    nlevels = kwargs.get('nlevels', 11)

    # Plot the maps
    for i in range(len(maps)):
        try:
            logger.info('Plotting model {} experiment {}'.format(models[i],
                                                                 exps[i]))
        except TypeError:
            logger.info('Plotting map {}'.format(i))

        # Contour plot
        cs = maps[i].plot.contourf(ax=axs[i], transform=ccrs.PlateCarree(),
                                   cmap='RdBu_r', levels=nlevels,
                                   add_colorbar=False, add_labels=False,
                                   extend='both', vmin=vmin, vmax=vmax)

        # Title
        if titles is not None:
            axs[i].set_title(titles[i])
        else:  # Use models and exps
            try:
                axs[i].set_title('{} {}'.format(models[i], exps[i]))
            except TypeError:
                logger.warning('No title for map {}'.format(i))

        # Coastlines
        axs[i].coastlines()

        # Longitude labels
        axs[i].set_xticks(np.arange(-180, 181, 60), crs=ccrs.PlateCarree())
        lon_formatter = cticker.LongitudeFormatter()
        axs[i].xaxis.set_major_formatter(lon_formatter)

        # Latitude labels
        axs[i].set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
        lat_formatter = cticker.LatitudeFormatter()
        axs[i].yaxis.set_major_formatter(lat_formatter)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    # Add the colorbar
    fig.colorbar(cs, cax=cbar_ax, orientation='horizontal')

    # Colorbar label
    cbar_label = kwargs.get('cbar_label', '')
    if cbar_label is not None:
        cbar_ax.set_xlabel(cbar_label)

    # Add a super title
    title = kwargs.get('title')
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save is True:
        outputdir = kwargs.get('outputdir', '.')
        filename = kwargs.get('filename', 'maps.png')
        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')


def single_map_plot(map=None, save=False, **kwargs):
    """
    Plot a single map (regression, correlation, etc.)
    An xarray.DataArray objects is expected
    and a map is plotted

    Args:
        map (xarray.DataArray): xarray.DataArray object
        save (bool, opt):       save the figure
        **kwargs:               additional arguments

    Kwargs:
        - loglevel (str,opt):   log level for the logger, default is 'WARNING'
        - model (str,opt):      model name
        - exp (str,opt):        experiment name
        - figsize (tuple,opt):  figure size, default is (11, 8.5)
        - nlevels (int,opt):    number of levels for the colorbar, default is 11
        - title (str,opt):      title for the figure
        - cb_label (str,opt):   label for the colorbar
        - outputdir (str,opt):  output directory for the figure, default is '.' (current directory)
        - filename (str,opt):   filename for the figure, default is 'maps.png'

    Raises:
        ValueError: if no map is provided
    """
    loglevel = kwargs.get('loglevel', 'WARNING')
    logger = log_configure(loglevel, 'Single map')

    if map is None:
        raise ValueError('Nothing to plot')

    # Add cyclic longitude
    map = add_cyclic_lon(map)

    # Evaluate min and max values for the common colorbar
    vmin, vmax = minmax_maps([map])
    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    model = kwargs.get('model')
    exp = kwargs.get('exp')

    # Generate the figure
    figsize = kwargs.get('figsize', (11, 8.5))

    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()},
                           figsize=figsize)

    # Set the number of levels for the colorbar
    nlevels = kwargs.get('nlevels', 10)

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
    title = kwargs.get('title')
    if title is not None:
        ax.set_title(title)
    else:
        try:
            ax.set_title('{} {}'.format(model, exp))
        except ValueError:
            logger.warning('No title for map')

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
    cbar_label = kwargs.get('cbar_label')
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
        outputdir = kwargs.get('outputdir', '.')

        # check the outputdir exists and create it if necessary
        if not os.path.exists(outputdir):
            logger.info('Creating output directory {}'.format(outputdir))
            os.makedirs(outputdir)
        try:
            filename = kwargs.get('filename')
        except ValueError:
            try:
                filename = model + '_' + exp + '.pdf'
            except ValueError:
                filename = 'map.pdf'

        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')


def maps_diffs_plot(maps=None, diffs=None, models=None, exps=None,
                    titles=None, save=False, **kwargs):
    """
    Plot maps (regression, correlation, etc.)
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):        list of xarray.DataArray objects
        diffs (list):       list of xarray.DataArray objects
        models (list):      list of models
        exps (list):        list of experiments
        titles (list, opt): list of titles for each map
                            overrides models and exps standard titles
        save (bool, opt):   save the figure
        **kwargs:           additional arguments

    Kwargs:
        - loglevel (str,opt):      log level for the logger, default is 'WARNING'
        - figsize (tuple,opt):     figure size, default is (11, 8.5)
        - vmin_diff (float,opt):   min value for the colorbar of the differences
        - vimax_diff (float,opt):  max value for the colorbar of the differences
        - nlevels (int,opt):       number of levels for the colorbar, default is 11
        - cbar_label (str,opt):    label for the colorbar
        - outputdir (str,opt):     output directory for the figure, default is '.' (current directory)
        - filename (str,opt):      filename for the figure, default is 'map.png'
    """
    loglevel = kwargs.get('loglevel', 'WARNING')
    logger = log_configure(loglevel, 'Multiple maps and differences')

    if maps is None:
        raise ValueError('Nothing to plot')

    if models is None and titles is None:
        logger.warning('No titles provided')
    if exps is None and titles is None:
        logger.warning('No titles provided')

    # Generate the figure
    nrows, ncols = plot_box(len(maps))
    figsize = kwargs.get('figsize', (11, 8.5))

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = minmax_maps(maps)
    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    if diffs is not None:
        vmin_diff = kwargs.get('vmin_diff', None)
        vmax_diff = kwargs.get('vmax_diff', None)

        if vmin_diff is None or vmax_diff is None:
            vmin_diff, vmax_diff = minmax_maps(diffs)
        logger.info('Min value for the colorbar (diffs): {}'.format(vmin_diff))
        logger.info('Max value for the colorbar (diffs): {}'.format(vmax_diff))

    # Drop unused axes
    for i in range(len(axs)):
        if i >= len(maps):
            axs[i].axis('off')
            logger.debug('Dropping unused axes {}'.format(i))

    # Set the number of levels for the colorbar
    nlevels = kwargs.get('nlevels', 11)

    # Plot the maps
    for i in range(len(maps)):
        try:
            logger.info('Plotting model {} experiment {}'.format(models[i],
                                                                 exps[i]))
        except TypeError:
            logger.info('Plotting map {}'.format(i))

        # Contour plot
        cs = maps[i].plot.contourf(ax=axs[i], transform=ccrs.PlateCarree(),
                                cmap='RdBu_r', levels=nlevels,
                                add_colorbar=False, add_labels=False,
                                extend='both', vmin=vmin, vmax=vmax)

        # Line contours with diffs
        if diffs is not None:
            try:
                ds = diffs[i].plot.contour(ax=axs[i], transform=ccrs.PlateCarree(),
                                      colors='k', levels=10, linewidths=0.5,
                                      vmin=vmin_diff, vmax=vmax_diff)

                axs[i].clabel(ds, fmt='%1.1f', fontsize=6, inline=True)
            except IndexError:
                logger.warning('No diff for map {}'.format(i))
        # Title
        if titles is not None:
            axs[i].set_title(titles[i])
        else:  # Use models and exps
            try:
                axs[i].set_title('{} {}'.format(models[i], exps[i]))
            except TypeError:
                logger.warning('No title for map {}'.format(i))

        # Coastlines
        axs[i].coastlines()

        # Longitude labels
        axs[i].set_xticks(np.arange(-180, 181, 60), crs=ccrs.PlateCarree())
        lon_formatter = cticker.LongitudeFormatter()
        axs[i].xaxis.set_major_formatter(lon_formatter)

        # Latitude labels
        axs[i].set_yticks(np.arange(-90, 91, 30), crs=ccrs.PlateCarree())
        lat_formatter = cticker.LatitudeFormatter()
        axs[i].yaxis.set_major_formatter(lat_formatter)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    # Add the colorbar
    fig.colorbar(cs, cax=cbar_ax, orientation='horizontal')

    # Colorbar label
    cbar_label = kwargs.get('cbar_label', '')
    if cbar_label is not None:
        cbar_ax.set_xlabel(cbar_label)

    # Add a super title
    title = kwargs.get('title')
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save is True:
        outputdir = kwargs.get('outputdir', '.')
        filename = kwargs.get('filename', 'maps.png')
        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')

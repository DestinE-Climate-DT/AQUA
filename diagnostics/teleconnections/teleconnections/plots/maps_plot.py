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
from .plot_utils import plot_box, add_cyclic_lon, evaluate_colorbar_limits


def maps_plot(maps=None, models=None, exps=None,
              titles=None, save=False, figsize=(11, 8.5),
              nlevels=12, cbar_label=None, title=None,
              sym=True, outputdir='.', filename='maps.png',
              loglevel='WARNING'):
    """Plot maps (regression, correlation, etc.)
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):          list of xarray.DataArray objects
        models (list):        list of models
        exps (list):          list of experiments
        titles (list, opt):   list of titles for each map
                              overrides models and exps standard titles
        save (bool, opt):     save the figure
        figsize (tuple,opt):  figure size, default is (11, 8.5)
        nlevels (int,opt):    number of levels for the colorbar, default is 12
        cbar_label (str,opt): label for the colorbar
        title (str,opt):      title for the figure (suptitle)
        sym (bool,opt):       symmetrical colorbar, default is True
        outputdir (str,opt):  output directory for the figure, default is '.' (current directory)
        filename (str,opt):   filename for the figure, default is 'maps.png'
        loglevel (str,opt):   log level for the logger, default is 'WARNING'
    """
    logger = log_configure(loglevel, 'Multiple maps')

    if maps is None:
        raise ValueError('Nothing to plot')

    if models is None and titles is None:
        logger.info('No titles provided')
    if exps is None and titles is None:
        logger.info('No titles provided')

    # Generate the figure
    nrows, ncols = plot_box(len(maps))

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)

    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    # Drop unused axes
    for i in range(len(axs)):
        if i >= len(maps):
            axs[i].axis('off')
            logger.debug('Dropping unused axes {}'.format(i))

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
    if cbar_label is not None:
        cbar_ax.set_xlabel(cbar_label)

    # Add a super title
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save is True:
        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')


def single_map_plot(map=None, save=False, model=None, exp=None,
                    figsize=(11, 8.5), nlevels=12, title=None,
                    cbar_label=None, outputdir='.', filename='maps.png',
                    sym=True, contour=True,
                    loglevel='WARNING', **kwargs):
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
        sym (bool,opt):         symmetrical colorbar, default is True
        contour (bool,opt):     plot contours, default is True
        loglevel (str,opt):     log level for the logger, default is 'WARNING'

    Keyword Args:
        transform_first (bool):  transform the data before plotting with cartopy
                                 default is False, True can solve ocean/land issues

    Raises:
        ValueError: if no map is provided
    """
    logger = log_configure(loglevel, 'Single map')

    if map is None:
        raise ValueError('Nothing to plot')

    # Add cyclic longitude
    map = add_cyclic_lon(map)

    vmin, vmax = evaluate_colorbar_limits(maps=[map], sym=sym)

    # Generate the figure)
    fig, ax = plt.subplots(subplot_kw={'projection': ccrs.PlateCarree()},
                           figsize=figsize)

    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    # Plot the map
    if model is not None and exp is not None:
        logger.info('Plotting model {} experiment {}'.format(model, exp))
    else:
        logger.debug('Plotting map')

    # Build the lon/lat grid
    try:
        lon, lat = np.meshgrid(map.lon, map.lat)
    except AttributeError:
        try:
            lon, lat = np.meshgrid(map.longitude, map.latitude)
        except AttributeError:
            raise AttributeError('No lon/lat or longitude/latitude in the map')

    # Contour plot
    transform_first = kwargs.get('transform_first', False) # cartopy kwarg
    logger.info('transform_first: {}'.format(transform_first))

    cmap = plt.get_cmap('RdBu_r')
    cmap.set_bad('white')

    if contour is True:
        logger.debug('Plotting contours')
        cs = ax.contourf(lon, lat, map, transform=ccrs.PlateCarree(),
                        cmap=cmap, levels=nlevels,
                        extend='both', vmin=vmin, vmax=vmax,
                        transform_first=transform_first)
    else:
        logger.debug('Not plotting contours')
        logger.debug('Transform first: {} is ignored'.format(transform_first))
        cs = ax.pcolormesh(lon, lat, map, transform=ccrs.PlateCarree(),
                            cmap=cmap, vmin=vmin, vmax=vmax)

    # Title
    if title is not None:
        ax.set_title(title)
    else:
        if model is not None and exp is not None:
            ax.set_title('{} {}'.format(model, exp))
        else:
            logger.warning('No title provided')

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
                filename = 'map.pdf'

        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')


def maps_diffs_plot(maps=None, diffs=None, models=None, exps=None,
                    titles=None, save=False, figsize=(11, 8.5),
                    vmin_diff=None, vmax_diff=None,
                    sym=True, nlevels=12,
                    cbar_label=None, title=None, outputdir='.',
                    filename='maps.png', loglevel='WARNING'):
    """
    Plot maps (regression, correlation, etc.)
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):             list of xarray.DataArray objects
        diffs (list):            list of xarray.DataArray objects
        models (list):           list of models
        exps (list):             list of experiments
        titles (list, opt):      list of titles for each map
                                 overrides models and exps standard titles
        save (bool, opt):        save the figure
        figsize (tuple,opt):     figure size, default is (11, 8.5)
        vmin_diff (float,opt):   min value for the colorbar of the differences
        vimax_diff (float,opt):  max value for the colorbar of the differences
        nlevels (int,opt):       number of levels for the colorbar, default is 12
        cbar_label (str,opt):    label for the colorbar
        outputdir (str,opt):     output directory for the figure, default is '.' (current directory)
        filename (str,opt):      filename for the figure, default is 'map.png'
        loglevel (str,opt):      log level for the logger, default is 'WARNING'
    """
    logger = log_configure(loglevel, 'Multiple maps and differences')

    if maps is None:
        raise ValueError('Nothing to plot')

    if models is None and titles is None:
        logger.warning('No titles provided')
    if exps is None and titles is None:
        logger.warning('No titles provided')

    # Generate the figure
    nrows, ncols = plot_box(len(maps))

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)
    logger.debug('Min value for the colorbar: {}'.format(vmin))
    logger.debug('Max value for the colorbar: {}'.format(vmax))

    if diffs is not None:
        if vmin_diff is None or vmax_diff is None:
            vmin_diff, vmax_diff = evaluate_colorbar_limits(maps=diffs,
                                                            sym=sym)
        logger.info('Min value for the colorbar (diffs): {}'.format(vmin_diff))
        logger.info('Max value for the colorbar (diffs): {}'.format(vmax_diff))

    # Drop unused axes
    for i in range(len(axs)):
        if i >= len(maps):
            axs[i].axis('off')
            logger.debug('Dropping unused axes {}'.format(i))

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
                ds = diffs[i].plot.contour(ax=axs[i],
                                           transform=ccrs.PlateCarree(),
                                           colors='k', levels=10,
                                           linewidths=0.5,
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
    if cbar_label is not None:
        cbar_ax.set_xlabel(cbar_label)

    # Add a super title
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save is True:
        logger.info('Saving figure to {}/{}'.format(outputdir, filename))
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')

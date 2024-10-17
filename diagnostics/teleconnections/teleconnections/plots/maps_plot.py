"""
Module to plot maps (regression, correlation, etc.)

Functions:
    maps_diffs_plot:    plot multiple maps and as contours
                        the differences previously computed
"""
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np

from aqua.logger import log_configure
from aqua.util import plot_box, evaluate_colorbar_limits


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
    logger.debug("Min value for the colorbar: %s", vmin)
    logger.debug("Max value for the colorbar: %s", vmax)

    if diffs is not None:
        if vmin_diff is None or vmax_diff is None:
            vmin_diff, vmax_diff = evaluate_colorbar_limits(maps=diffs,
                                                            sym=sym)
        logger.debug("Min value for the colorbar (diffs): %s", vmin_diff)
        logger.debug("Max value for the colorbar (diffs): %s", vmax_diff)

    # Drop unused axes
    for i, ax in enumerate(axs):
        if i >= len(maps):
            ax.axis('off')
            logger.debug('Dropping unused axes %d', i)

    # Plot the maps
    for i, data_map in enumerate(maps):
        try:
            logger.info('Plotting model %s exp %s', models[i], exps[i])
        except TypeError:
            logger.info('Plotting map %d', i)

        # Contour plot
        cs = data_map.plot.contourf(ax=axs[i], transform=ccrs.PlateCarree(),
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
                logger.warning('No diff for map %d', i)
        # Title
        if titles is not None:
            axs[i].set_title(titles[i])
        else:  # Use models and exps
            try:
                axs[i].set_title(f'{models[i]} {exps[i]}')
            except TypeError:
                logger.warning('No title for map  n°%d', i)

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
        logger.info("Saving figure to %s/%s", outputdir, filename)
        fig.savefig('{}/{}'.format(outputdir, filename), format='pdf',
                    dpi=300, bbox_inches='tight')

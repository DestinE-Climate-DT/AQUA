"""
Module to plot multiple maps

"""
import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr
import os

from aqua.logger import log_configure
from aqua.util import plot_box, add_cyclic_lon, set_ticks
from aqua.util import coord_names, evaluate_colorbar_limits
from aqua.util import cbar_get_label, create_folder


def plot_maps(maps: list = None,
              titles: list = None,
              contour=True,
              save=False, sym=False,
              figsize=(11, 8.5),
              nlevels=11, outputdir='.',
              cmap='RdBu_r',
              gridlines=False,
              display=True,
              loglevel='WARNING',
              **kwargs):
    """
    Plot multiple maps.
    This is supposed to be used for maps to be compared together.
    A list of xarray.DataArray objects is expected
    and a map is plotted for each of them

    Args:
        maps (list):          list of xarray.DataArray objects
        titles (list,opt):    list of titles for the maps
        contour (bool,opt):   If True, plot a contour map,otherwise a pcolormesh. Defaults to True.
        save (bool,opt):      save the figure, default is False
        sym (bool,opt):       symetric colorbar, default is False
        figsize (tuple,opt):  figure size, default is (11, 8.5)
        nlevels (int,opt):    number of levels for the colorbar, default is 11
        outputdir (str,opt):  output directory, default is '.'
        vmin (float,opt):     minimum value for the colorbar, default is None
        vmax (float,opt):     maximum value for the colorbar, default is None
        cmap (str,opt):       colormap, default is 'RdBu_r'
        gridlines (bool,opt): display gridlines, default is False
        display (bool,opt):   display the figure, default is True
        loglevel (str,opt):   log level, default is 'WARNING'
        **kwargs:             additional arguments

    Keyword Args:
        title (str,opt):      super title for the figure
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to False.
        vmin (float, optional): minimum value for the colorbar
        vmax (float, optional): maximum value for the colorbar
        cbar_label (str,opt): colorbar label
        dpi (int,opt):        dots per inch, default is 100
        models (list,opt):    list of models
        exps (list,opt):      list of experiments
        filename (str,opt):   filename for the figure, default is 'maps.pdf'
        format (str,opt):     format for the figure, default is 'pdf'
        nxticks (int,opt):    number of xticks, default is 7
        nyticks (int,opt):    number of yticks, default is 7
        ticks_rounding (int, optional):  Number of digits to round the ticks.
        cyclic_lon (bool,opt): add cyclic longitude, default is True

    Raises:
        ValueError: if nothing to plot, i.e. maps is None or not a list of xarray.DataArray

    Return:
        fig, axs if more manipulations on the figure are needed
    """
    logger = log_configure(loglevel, 'Multiple maps')

    if maps is None:
        raise ValueError('Nothing to plot')
    else:  # Check if maps is a list of xarray.DataArray
        for data_map in maps:
            if not isinstance(data_map, xr.DataArray):
                raise ValueError('maps should be a list of xarray.DataArray')

    # Generate the figure
    nrows, ncols = plot_box(len(maps))
    logger.debug('Creating a %d x %d grid', nrows, ncols)

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = evaluate_colorbar_limits(maps=maps, sym=sym)
    vmin = kwargs.get('vmin', vmin)
    vmax = kwargs.get('vmax', vmax)

    logger.debug("Min value for the colorbar: %s", vmin)
    logger.debug("Max value for the colorbar: %s", vmax)

    if contour:
        levels = np.linspace(vmin, vmax, nlevels + 1)

    # Option to transform the data before plotting
    transform_first = kwargs.get('transform_first', False)
    cyclic_lon = kwargs.get('cyclic_lon', True)
    models = kwargs.get('models', None)
    exps = kwargs.get('exps', None)
    proj = ccrs.PlateCarree()

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

        logger.info("Loading data for map %d", i)
        data_map = data_map.load(keep_attrs=True)

        if cyclic_lon is True:
            logger.info('Adding cyclic longitude')
            try:
                data_map = add_cyclic_lon(data_map)
            except Exception as e:
                logger.debug('Error adding cyclic longitude: %s', e)
                logger.warning('Cyclic longitude not added, it can be set to False with the cyclic_lon option')

        # Get the coordinate names
        lon_name, lat_name = coord_names(data_map)

        if contour:
            # grid lon and lat
            lon, lat = np.meshgrid(data_map[lon_name],
                                   data_map[lat_name])

            cs = axs[i].contourf(lon, lat, data_map,
                                 transform=proj,
                                 cmap=cmap, levels=levels,
                                 add_colorbar=False, add_labels=False,
                                 extend='both',
                                 transform_first=transform_first)
        else:
            cs = axs[i].pcolormesh(data_map[lon_name],
                                   data_map[lat_name],
                                   data_map,
                                   transform=proj,
                                   cmap=cmap,
                                   vmin=vmin, vmax=vmax,
                                   add_colorbar=False,
                                   add_labels=False)

        # Title
        if titles is not None:
            axs[i].set_title(titles[i])
        else:  # Use models and exps
            try:
                axs[i].set_title('{} {}'.format(models[i], exps[i]))
            except TypeError:
                logger.warning('No title for map n°%s', i)

        # Coastlines
        logger.debug('Adding coastlines')
        axs[i].coastlines()

        if gridlines:
            logger.debug('Adding gridlines')
            axs[i].gridlines()

        # Longitude labels
        # Evaluate the longitude ticks
        nxticks = kwargs.get('nxticks', 7)
        nyticks = kwargs.get('nyticks', 7)
        ticks_rounding = kwargs.get('ticks_rounding', None)
        if ticks_rounding:
            logger.debug('Setting ticks rounding to %s', ticks_rounding)

        fig, axs[i] = set_ticks(data=data_map, fig=fig, ax=axs[i],
                                nticks=(nxticks, nyticks),
                                ticks_rounding=ticks_rounding,
                                lon_name=lon_name, lat_name=lat_name,
                                proj=proj, loglevel=loglevel)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    cbar_label = cbar_get_label(data=maps[0],
                                cbar_label=kwargs.get('cbar_label', None),
                                loglevel=loglevel)
    logger.debug('Setting colorbar label to %s', cbar_label)

    # Add the colorbar
    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make the colorbar ticks symmetrical if sym=True
    if sym:
        logger.debug('Setting colorbar ticks to be symmetrical')
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
    else:
        cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

    # Set x-y labels
    for ax in axs:
        ax.set_xlabel('Longitude [deg]')
        ax.set_ylabel('Latitude [deg]')

    title = kwargs.get('title', None)
    # Add a super title
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save is True:
        logger.debug('Saving figure to %s', outputdir)
        create_folder(outputdir, loglevel=loglevel)
        filename = kwargs.get('filename', 'maps')
        plot_format = kwargs.get('format', 'pdf')
        filename = f"{filename}.{plot_format}"

        logger.info("Saving figure to %s/%s", outputdir, filename)

        if contour:
            dpi = kwargs.get('dpi', 300)
        else:
            dpi = kwargs.get('dpi', 100)
            if dpi == 100:
                logger.info("Setting dpi to 100 by default, use dpi kwarg to change it")

        fig.savefig('{}/{}'.format(outputdir, filename),
                    dpi=dpi, bbox_inches='tight')

    if display is False:
        logger.debug("Display is set to False, closing figure")
        plt.close(fig)

    return fig, axs


def plot_maps_diff(maps: list,
                       maps_ref: list,
                       titles: list = None,
                       contour=True,
                       save=False, sym=False, sym_contour=False,
                       figsize=(11, 8.5),
                       nlevels=11, outputdir='.',
                       cmap='RdBu_r',
                       gridlines=False,
                       display=True,
                       loglevel='WARNING',
                       **kwargs):
    """
    Plot the difference of multiple maps.
    This is supposed to be used for maps to be compared together.
    Two lists of xarray.DataArray objects are expected
    and the difference (maps[i] - maps_ref[i]) is plotted for each pair.

    Args:
        maps (list):          list of xarray.DataArray objects
        maps_ref (list):      list of xarray.DataArray reference objects
        titles (list,opt):    list of titles for the maps
        contour (bool,opt):   If True, plot a contour map, otherwise a pcolormesh. Defaults to True.
        save (bool,opt):      save the figure, default is False
        sym (bool,opt):       symetric colorbar for the difference maps, default is False
        sym_contour (bool,opt): symmetric colorbar for the contour, default is False
        figsize (tuple,opt):  figure size, default is (11, 8.5)
        nlevels (int,opt):    number of levels for the colorbar, default is 11
        outputdir (str,opt):  output directory, default is '.'
        cmap (str,opt):       colormap, default is 'RdBu_r'
        gridlines (bool,opt): display gridlines, default is False
        display (bool,opt):   display the figure, default is True
        loglevel (str,opt):   log level, default is 'WARNING'
        **kwargs:             additional arguments

    Keyword Args:
        title (str,opt):      super title for the figure
        transform_first (bool, optional): If True, transform the data before plotting. Defaults to False.
        vmin_fill (float, optional): minimum value for the colorbar of the fill
        vmax_fill (float, optional): maximum value for the colorbar of the fill
        vmin_contour (float, optional): minimum value for the colorbar of the contour
        vmax_contour (float, optional): maximum value for the colorbar of the contour
        cbar_label (str,opt): colorbar label
        dpi (int,opt):        dots per inch, default is 100
        models (list,opt):    list of models
        exps (list,opt):      list of experiments
        filename (str,opt):   filename for the figure, default is 'maps.pdf'
        format (str,opt):     format for the figure, default is 'pdf'
        nxticks (int,opt):    number of xticks, default is 7
        nyticks (int,opt):    number of yticks, default is 7
        ticks_rounding (int, optional):  Number of digits to round the ticks.
        cyclic_lon (bool,opt): add cyclic longitude, default is True

    Raises:
        ValueError: if nothing to plot, i.e. maps is None or not a list of xarray.DataArray

    Return:
        fig, axs if more manipulations on the figure are needed
    """
    logger = log_configure(loglevel, 'Multiple maps difference')

    if maps is None or maps_ref is None:
        raise ValueError('Nothing to plot')
    elif len(maps) != len(maps_ref):
        raise ValueError('Maps and reference maps should have the same length')
    else:  # Check if maps and maps_ref are lists of xarray.DataArray
        for data_map, data_map_ref in zip(maps, maps_ref):
            if not isinstance(data_map, xr.DataArray) or not isinstance(data_map_ref, xr.DataArray):
                raise ValueError('maps and maps_ref should be lists of xarray.DataArray')

    # Calculate differences
    diffs = [data_map - data_map_ref for data_map, data_map_ref in zip(maps, maps_ref)]

    # Generate the figure
    nrows, ncols = plot_box(len(diffs))
    logger.debug('Creating a %d x %d grid', nrows, ncols)

    fig, axs = plt.subplots(nrows=nrows, ncols=ncols,
                            subplot_kw={'projection': ccrs.PlateCarree()},
                            figsize=figsize)
    axs = axs.flatten()

    # Evaluate min and max values for the common colorbar
    vmin, vmax = evaluate_colorbar_limits(maps=diffs, sym=sym)
    vmin = kwargs.get('vmin_fill', vmin)
    vmax = kwargs.get('vmax_fill', vmax)

    logger.debug("Min value for the colorbar: %s", vmin)
    logger.debug("Max value for the colorbar: %s", vmax)

    if contour:
        levels = np.linspace(vmin, vmax, nlevels + 1)

    # Option to transform the data before plotting
    transform_first = kwargs.get('transform_first', False)
    cyclic_lon = kwargs.get('cyclic_lon', True)
    models = kwargs.get('models', None)
    exps = kwargs.get('exps', None)
    proj = ccrs.PlateCarree()

    # Drop unused axes
    for i, ax in enumerate(axs):
        if i >= len(diffs):
            ax.axis('off')
            logger.debug('Dropping unused axes %d', i)

    # Plot the maps
    for i, diff_map in enumerate(diffs):
        try:
            logger.info('Plotting model %s exp %s', models[i], exps[i])
        except TypeError:
            logger.info('Plotting map %d', i)

        logger.info("Loading data for map %d", i)
        diff_map = diff_map.load(keep_attrs=True)

        if cyclic_lon:
            logger.info('Adding cyclic longitude')
            try:
                diff_map = add_cyclic_lon(diff_map)
            except Exception as e:
                logger.debug('Error adding cyclic longitude: %s', e)
                logger.warning('Cyclic longitude not added, it can be set to False with the cyclic_lon option')

        # Get the coordinate names
        lon_name, lat_name = coord_names(diff_map)

        if contour:
            # grid lon and lat
            lon, lat = np.meshgrid(diff_map[lon_name],
                                   diff_map[lat_name])

            cs = axs[i].contourf(lon, lat, diff_map,
                                 transform=proj,
                                 cmap=cmap, levels=levels,
                                 add_colorbar=False, add_labels=False,
                                 extend='both',
                                 transform_first=transform_first)
        else:
            cs = axs[i].pcolormesh(diff_map[lon_name],
                                   diff_map[lat_name],
                                   diff_map,
                                   transform=proj,
                                   cmap=cmap,
                                   vmin=vmin, vmax=vmax)

        # Title
        if titles is not None:
            axs[i].set_title(titles[i])
        else:  # Use models and exps
            try:
                axs[i].set_title('{} {}'.format(models[i], exps[i]))
            except TypeError:
                logger.warning('No title for map n°%s', i)

        # Coastlines
        logger.debug('Adding coastlines')
        axs[i].coastlines()

        if gridlines:
            logger.debug('Adding gridlines')
            axs[i].gridlines()

        # Longitude labels
        # Evaluate the longitude ticks
        nxticks = kwargs.get('nxticks', 7)
        nyticks = kwargs.get('nyticks', 7)
        ticks_rounding = kwargs.get('ticks_rounding', None)
        if ticks_rounding:
            logger.debug('Setting ticks rounding to %s', ticks_rounding)

        fig, axs[i] = set_ticks(data=diff_map, fig=fig, ax=axs[i],
                                nticks=(nxticks, nyticks),
                                ticks_rounding=ticks_rounding,
                                lon_name=lon_name, lat_name=lat_name,
                                proj=proj, loglevel=loglevel)

        # Plot the original map as contour
        if contour:
            logger.info("Plotting the original map as contour for map %d", i)
            if cyclic_lon:
                try:
                    maps[i] = add_cyclic_lon(maps[i])
                except Exception as e:
                    logger.error("Cannot add cyclic longitude to the original map: %s", e)

            # Evaluate vmin and vmax of the contour
            vmin_contour = kwargs.get('vmin_contour', None)
            vmax_contour = kwargs.get('vmax_contour', None)

            if vmin_contour is None or vmax_contour is None:
                vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[maps[i]], sym=sym_contour)
            else:
                if sym_contour:
                    logger.warning("sym_contour=True, vmin_contour and vmax_contour given will be ignored")
                    vmin_contour, vmax_contour = evaluate_colorbar_limits(maps=[maps[i]], sym=sym_contour)

            levels_contour = np.linspace(vmin_contour, vmax_contour, nlevels)

            # Add the contour
            lon, lat = np.meshgrid(maps[i][lon_name], maps[i][lat_name])

            if transform_first:
                cs2 = axs[i].contour(lon, lat, maps[i],
                                     levels=levels_contour,
                                     colors='black', linewidths=0.5,
                                     transform=proj)
            else:
                cs2 = axs[i].contour(maps[i][lon_name], maps[i][lat_name],
                                     maps[i],
                                     levels=levels_contour,
                                     colors='black', linewidths=0.5,
                                     transform=proj)

            axs[i].clabel(cs2, cs2.levels, inline=True, fontsize=8)

    # Super title
    title = kwargs.get('title', None)
    if title is not None:
        fig.suptitle(title, fontsize=14)


# Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.05, 0.6, 0.02])

    cbar_label = cbar_get_label(data=maps[0],
                                cbar_label=kwargs.get('cbar_label', None),
                                loglevel=loglevel)
    logger.debug('Setting colorbar label to %s', cbar_label)

    # Add the colorbar
    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make the colorbar ticks symmetrical if sym=True
    if sym:
        logger.debug('Setting colorbar ticks to be symmetrical')
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))
    else:
        cbar.set_ticks(np.linspace(vmin, vmax, nlevels + 1))

    # Set x-y labels
    for ax in axs:
        ax.set_xlabel('Longitude [deg]')
        ax.set_ylabel('Latitude [deg]')

    title = kwargs.get('title', None)

    # Add a super title
    if title is not None:
        fig.suptitle(title, fontsize=16)

    # Save the figure
    if save:
        filename = kwargs.get('filename', 'maps.pdf')
        format = kwargs.get('format', 'pdf')
        dpi = kwargs.get('dpi', 100)

        # Set the format
        if '.' in filename:
            format = filename.split('.')[-1]
        else:
            filename += '.' + format

        # Join the output directory
        filename = os.path.join(outputdir, filename)

        logger.info("Saving figure as %s", filename)
        fig.savefig(filename, format=format, dpi=dpi)

    # Display the figure
    if display:
        logger.debug('Displaying the figure')
        plt.show()

    return fig, axs

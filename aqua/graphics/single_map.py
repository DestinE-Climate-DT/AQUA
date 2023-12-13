import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.logger import log_configure
from aqua.util import create_folder
from aqua.util import add_cyclic_lon, evaluate_colorbar_limits
from aqua.util import cbar_get_label, set_map_title
from aqua.util import check_coordinates, coord_names


def plot_single_map(data: xr.DataArray,
                    save=False,
                    contour=True, sym=False,
                    figsize=(11, 8.5),
                    nlevels=11, outputdir=".",
                    vmin=None, vmax=None,
                    cmap='RdBu_r',
                    gridlines=False,
                    display=True,
                    loglevel='WARNING',
                    **kwargs):
    """
    Plot contour or pcolormesh map of a single variable.

    Args:
        data (xr.DataArray):       Data to plot.
        save (bool, optional):     If True, save the figure. Defaults to False.
        contour (bool, optional):  If True, plot a contour map,
                                   otherwise a pcolormesh.
                                   Defaults to True.
        figsize (tuple, optional): Figure size. Defaults to (11, 8.5).
        nlevels (int, optional):   Number of levels for the contour map.
                                   Defaults to 11.
        outputdir (str, optional): Output directory. Defaults to ".".
        vmin (float, optional):    Minimum value for the colorbar.
                                   Defaults to None.
        vmax (float, optional):    Maximum value for the colorbar.
                                   Defaults to None.
        cmap (str, optional):      Colormap. Defaults to 'RdBu_r'.
        gridlines (bool, optional): If True, plot gridlines. Defaults to False.
        display (bool, optional):  If True, display the figure. Defaults to True.
        loglevel (str, optional):  Log level. Defaults to 'WARNING'.

    Keyword Args:
        title (str, optional):       Title of the figure. Defaults to None.
        transform_first (bool, optional): If True, transform the data before
                                          plotting. Defaults to False.
        cbar_label (str, optional):  Colorbar label. Defaults to None.
        dpi (int, optional):         Dots per inch. Defaults to 100.
        model (str, optional):       Model name. Defaults to None.
        exp (str, optional):         Experiment name. Defaults to None.
        filename (str, optional):    Filename. Defaults to 'map'.
        format (str, optional):      Format of the figure. Defaults to 'pdf'.
        nxticks (int, optional):     Number of x ticks. Defaults to 7.
        nyticks (int, optional):     Number of y ticks. Defaults to 7.
        cyclic_lon (bool, optional): If True, add cyclic longitude.

    Raises:
        ValueError: If data is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map')

    # We load in memory the data, to avoid problems with dask
    logger.info("Loading data in memory")
    data = data.load(keep_attrs=True)

    cycling = kwargs.get('cyclic_lon', True)
    if cycling:
        logger.info("Adding cyclic longitude")
        try:
            data = add_cyclic_lon(data)
        except Exception as e:
            logger.error("Cannot add cyclic longitude: %s", e)
            logger.warning("Cyclic longitude can be set to False with the cyclic_lon kwarg")

    proj = ccrs.PlateCarree()

    logger.debug("Setting figsize to %s", figsize)
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection=proj)

    # Evaluate vmin and vmax if not given
    if vmin is None or vmax is None:
        vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    else:
        if sym:
            logger.warning("sym=True, vmin and vmax given will be ignored")
            vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    logger.debug("Setting vmin to %s, vmax to %s", vmin, vmax)
    if contour:
        levels = np.linspace(vmin, vmax, nlevels + 1)

    # Get the coordinate names
    lon_name, lat_name = coord_names(data)

    # Plot the data
    if contour:
        # grid lon and lat
        lon, lat = np.meshgrid(data[lon_name], data[lat_name])

        transform_first = kwargs.get('transform_first', False)
        cs = ax.contourf(lon, lat, data, cmap=cmap,
                         transform=proj, levels=levels,
                         extend='both',
                         transform_first=transform_first)
    else:
        cs = ax.pcolormesh(data[lon_name], data[lat_name], data, cmap=cmap,
                           transform=proj, vmin=vmin, vmax=vmax)

    logger.debug("Adding coastlines")
    ax.coastlines()

    if gridlines:
        logger.debug("Adding gridlines")
        ax.gridlines()

    # Longitude labels
    # Evaluate the longitude ticks
    nxticks = kwargs.get('nxticks', 7)
    try:
        lon_min = data[lon_name].values.min()
        lon_max = data[lon_name].values.max()
        (lon_min, lon_max), _ = check_coordinates(lon=(lon_min, lon_max),
                                                  default={"lon_min": -180,
                                                           "lon_max": 180,
                                                           "lat_min": -90,
                                                           "lat_max": 90},)
        logger.debug("Setting longitude ticks from %s to %s", lon_min, lon_max)
    except KeyError:
        logger.critical("No longitude coordinate found, setting default values")
        lon_min = -180
        lon_max = 180
    step = (lon_max - lon_min) / (nxticks - 1)
    xticks = np.arange(lon_min, lon_max + 1, step)
    logger.debug("Setting longitude ticks to %s", xticks)
    ax.set_xticks(xticks, crs=proj)
    lon_formatter = cticker.LongitudeFormatter()
    ax.xaxis.set_major_formatter(lon_formatter)

    # Latitude labels
    # Evaluate the latitude ticks
    nyticks = kwargs.get('nyticks', 7)
    try:
        lat_min = data[lat_name].values.min()
        lat_max = data[lat_name].values.max()
        _, (lat_min, lat_max) = check_coordinates(lat=(lat_min, lat_max),
                                                  default={"lon_min": -180,
                                                           "lon_max": 180,
                                                           "lat_min": -90,
                                                           "lat_max": 90},)
        logger.debug("Setting latitude ticks from %s to %s", lat_min, lat_max)
    except KeyError:
        logger.critical("No latitude coordinate found, setting default values")
        lat_min = -90
        lat_max = 90
    step = (lat_max - lat_min) / (nyticks - 1)
    yticks = np.arange(lat_min, lat_max + 1, step)
    ax.set_yticks(yticks, crs=proj)
    lat_formatter = cticker.LatitudeFormatter()
    ax.yaxis.set_major_formatter(lat_formatter)

    # Adjust the location of the subplots on the page to make room for the colorbar
    fig.subplots_adjust(bottom=0.25, top=0.9, left=0.05, right=0.95,
                        wspace=0.1, hspace=0.5)

    # Add a colorbar axis at the bottom of the graph
    cbar_ax = fig.add_axes([0.2, 0.15, 0.6, 0.02])

    cbar_label = cbar_get_label(data,
                                cbar_label=kwargs.get('cbar_label', None),
                                loglevel=loglevel)
    logger.debug("Setting colorbar label to %s", cbar_label)

    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make tick of colorbar simmetric if sym=True
    if sym:
        logger.debug("Setting colorbar ticks to be symmetrical")
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels + 1))

    # Set x-y labels
    ax.set_xlabel('Longitude [deg]')
    ax.set_ylabel('Latitude [deg]')

    # Set title
    title = set_map_title(data, title=kwargs.get('title', None),
                          model=kwargs.get('model', None),
                          exp=kwargs.get('exp', None),
                          loglevel=loglevel)

    if title:
        logger.debug("Setting title to %s", title)
        ax.set_title(title)

    # Saving
    if save:
        logger.debug("Saving figure to %s", outputdir)
        create_folder(outputdir, loglevel=loglevel)
        filename = kwargs.get('filename', 'map')
        plot_format = kwargs.get('format', 'pdf')
        filename = f"{filename}.{plot_format}"
        logger.debug("Setting filename to %s", filename)

        logger.info("Saving figure as %s/%s", outputdir, filename)
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

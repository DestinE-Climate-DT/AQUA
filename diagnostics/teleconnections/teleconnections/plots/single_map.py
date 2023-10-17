import cartopy.crs as ccrs
import cartopy.mpl.ticker as cticker
import matplotlib.pyplot as plt
import numpy as np
import xarray as xr

from aqua.logger import log_configure
from aqua.util import create_folder
from .plot_utils import add_cyclic_lon, evaluate_colorbar_limits
from .plot_utils import cbar_get_label, set_map_title


def plot_single_map(data: xr.DataArray,
                    save=False,
                    contour=True, sym=True,
                    figsize=(11, 8.5),
                    nlevels=11, outputdir=".",
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
        loglevel (str, optional):  Log level. Defaults to 'WARNING'.

    Keyword Args:
        transform_first (bool, optional): If True, transform the data before
                                          plotting. Defaults to False.
        cbar_label (str, optional): Colorbar label. Defaults to None.
        dpi (int, optional):        Dots per inch. Defaults to 100.
        model (str, optional):      Model name. Defaults to None.
        exp (str, optional):        Experiment name. Defaults to None.
        filename (str, optional):   Filename. Defaults to 'map'.
        format (str, optional):     Format of the figure. Defaults to 'pdf'.

    Raises:
        ValueError: If data is not a DataArray.
    """
    logger = log_configure(loglevel, 'plot_single_map')

    # We load in memory the data, to avoid problems with dask
    logger.debug("Loading data in memory")
    data = data.load(keep_attrs=True)

    logger.debug("Adding cyclic longitude")
    data = add_cyclic_lon(data)

    proj = ccrs.PlateCarree()

    logger.debug("Setting figsize to %s", figsize)
    fig = plt.figure(figsize=figsize)
    ax = fig.add_subplot(111, projection=proj)

    vmin, vmax = evaluate_colorbar_limits(maps=[data], sym=sym)
    logger.debug("Setting vmin to %s", vmin)
    logger.debug("Setting vmax to %s", vmax)
    if contour:
        levels = np.linspace(vmin, vmax, nlevels+1)

    # Plot the data
    if contour:
        # grid lon and lat
        lon, lat = np.meshgrid(data['lon'], data['lat'])

        transform_first = kwargs.get('transform_first', False)
        cs = ax.contourf(lon, lat, data, cmap='RdBu_r',
                         transform=proj, levels=levels,
                         extend='both',
                         transform_first=transform_first)
    else:
        cs = ax.pcolormesh(data['lon'], data['lat'], data, cmap='RdBu_r',
                           transform=proj, vmin=vmin, vmax=vmax)

    logger.debug("Adding coastlines")
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

    cbar_label = cbar_get_label(data,
                                cbar_label=kwargs.get('cbar_label', None),
                                loglevel=loglevel)
    logger.debug("Setting colorbar label to %s", cbar_label)

    cbar = fig.colorbar(cs, cax=cbar_ax, orientation='horizontal',
                        label=cbar_label)

    # Make tick of colorbar simmetric if sym=True
    if sym:
        logger.debug("Setting colorbar ticks to be symmetrical")
        cbar.set_ticks(np.linspace(-vmax, vmax, nlevels+1))

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
        format = kwargs.get('format', 'pdf')
        filename = f"{filename}.{format}"
        logger.debug("Setting filename to %s", filename)

        logger.info("Saving figure as %s/%s", outputdir, filename)
        if contour:
            dpi = kwargs.get('dpi', 300)
        else:
            dpi = kwargs.get('dpi', 100)
            if dpi == 100:
                logger.warning("Setting dpi to 100 by default, use dpi kwarg to change it")

        fig.savefig('{}/{}'.format(outputdir, filename),
                    dpi=dpi, bbox_inches='tight')

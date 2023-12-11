"""Graphics utilities for Aqua."""
import math

import xarray as xr
import cartopy.util as cutil


def add_cyclic_lon(da: xr.DataArray):
    """
    Add a cyclic longitude point to a DataArray using cartopy
    and preserving the DataArray as data structure.

    It assumes that the longitude coordinate is named 'lon' and
    the latitude coordinate is named 'lat'.

    Parameters:
    da (xarray.DataArray): Input data array with longitude coordinate

    Returns:
    xarray.DataArray: The same input data array with the cyclic point added
                      along longitude
    """
    if not isinstance(da, xr.DataArray) or da is None:
        raise ValueError("Input must be an xarray.DataArray object.")

    # Support both lon and longitude names
    lon_name, lat_name = coord_names(da)

    cyclic_da, cyclic_lon = cutil.add_cyclic_point(da, coord=da[lon_name])

    # update the longitude coordinate with cyclic longitude
    new_da = xr.DataArray(cyclic_da, dims=da.dims)
    new_da = new_da.assign_coords(lon=cyclic_lon)
    new_da = new_da.assign_coords(lat=da[lat_name])

    # TODO: add old attributes to the new DataArray

    return new_da


def plot_box(num_plots=0):
    """
    Evaluate the number of rows and columns for a plot
    based on the number of plots to be plotted.

    Args:
        num_plots (int): Number of plots to be plotted.

    Returns:
        num_rows (int): Number of rows for the plot.
        num_cols (int): Number of columns for the plot.

    Raises:
        ValueError: If the number of plots is 0.
    """
    if num_plots == 0:
        raise ValueError('Number of plots must be greater than 0.')

    num_cols = math.ceil(math.sqrt(num_plots))
    num_rows = math.ceil(num_plots / num_cols)

    return num_rows, num_cols


def minmax_maps(maps: list):
    """
    Find the minimum and maximum values of the maps values
    for a list of maps.

    After finding the minimum and maximum values,
    values are made simmetric around 0.

    Args:
        regs (list): List of maps.

    Returns:
        vmin (float): Minimum value of the colorbar.
        vmax (float): Maximum value of the colorbar.
    """

    minmax = (min([map.min().values for map in maps]),
              max([map.max().values for map in maps]))

    vmin = minmax[0]
    vmax = minmax[1]

    # Make values simmetric around 0
    absmax = max(abs(vmin), abs(vmax))
    vmin = -absmax
    vmax = absmax

    return vmin, vmax


def evaluate_colorbar_limits(maps: list, sym: bool = True):
    """
    Evaluate the minimum and maximum values of the colorbar

    Args:
        maps (list):     List of maps.
        sym (bool, opt): If True, the colorbar is symmetrical around 0.

    Returns:
        vmin (float): Minimum value of the colorbar.
        vmax (float): Maximum value of the colorbar.
    """
    if maps is None:
        raise ValueError('maps must be specified.')

    if sym:
        vmin, vmax = minmax_maps(maps)
    else:
        vmin = min([map.min().values for map in maps])
        vmax = max([map.max().values for map in maps])

    return vmin, vmax


def cbar_get_label(data: xr.DataArray, cbar_label: str = None,
                   loglevel='WARNING'):
    """
    Evaluate the colorbar label.

    Args:
        data (xarray.DataArray): Input data array.
        cbar_label (str, opt):   Colorbar label.
        loglevel (str, opt):     Log level.

    Returns:
        cbar_label (str): Colorbar label.
    """
    logger = log_configure(loglevel, 'cbar get label')

    if cbar_label is None:
        cbar_label = getattr(data, 'long_name', None)
        if cbar_label is None:
            cbar_label = getattr(data, 'short_name', None)
        if cbar_label is None:
            cbar_label = getattr(data, 'shortName', None)
        logger.debug("Using %s as colorbar label", cbar_label)

        units = getattr(data, 'units', None)

        if units:
            cbar_label = f"{cbar_label} [{units}]"
            logger.debug("Adding units to colorbar label")

    if cbar_label is None:
        logger.warning("No colorbar label found, please specify one with the cbar_label argument.")

    return cbar_label


def set_map_title(data: xr.DataArray, title: str = None,
                  model: str = None, exp: str = None,
                  loglevel='WARNING'):
    """
    Evaluate the map title.

    Args:
        data (xarray.DataArray): Input data array.
        title (str, opt):        Map title.
        loglevel (str, opt):     Log level.

    Returns:
        title (str): Map title.
    """
    logger = log_configure(loglevel, 'set map title')

    if title is None:
        try:
            title = data.long_name
            logger.debug("Using long_name as map title")

            try:
                logger.debug("Adding time to map title")
                title = f"{title} {data.time.values}"
            except AttributeError:
                logger.debug("No time found")
        except AttributeError:
            if model is not None and exp is not None:
                title = f"{model} {exp}"
                logger.debug("Using model and exp as map title")

    return title


def coord_names(data: xr.DataArray):
    """
    Get the names of the longitude and latitude coordinates.

    Args:
        data (xarray.DataArray): Input data array.

    Returns:
        lon_name (str): Name of the longitude coordinate.
        lat_name (str): Name of the latitude coordinate.
    """
    try:
        lon_name = 'lon'
        data.lon
    except AttributeError:
        lon_name = 'longitude'
        data.longitude
    try:
        lat_name = 'lat'
        data.lat
    except AttributeError:
        lat_name = 'latitude'
        data.latitude

    return lon_name, lat_name

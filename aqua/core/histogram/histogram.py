import numpy as np
import xarray as xr
import builtins
import dask.array as da
from aqua.core.util import convert_data_units
from aqua.core.logger import log_configure

def histogram(data: xr.DataArray, bins = 10, range = None, units = None,
              weighted = True, loglevel='WARNING', dask=True, check=False, density=False):
    """
    Function to calculate a histogram of a DataArray.

    Args:
        data (xarray.Dataset):     The input DataArray. If it is a Dataset, the first variable is used.
        bins (int, optional):      The number of bins for the histogram. Defaults to 10.
        range (tuple, optional):   The lower and upper range of the bins. Defaults to None (in that case it is determined automatically).
        weighted (bool, optional): Use latitudinal weights for the histogram. Defaults to True.
        dask (bool, optional):     If True, uses Dask for parallel computation. Defaults to True.
        units (str, optional):     Convert data to these units. Defaults to None.
        check (bool, optional):    Checks if the sum of counts in the histogram is equal to the size of the data. 
                                   Defaults to False. This forces the histogram to be computed.
        density (bool, optional):  Returns a probability density function,
                                   normalized such that the integral over the range is 1. Defaults to False.
        loglevel (str, optional):  Logging level. Defaults to 'WARNING'.

    Raises:
        TypeError: If the input data is not an xarray DataArray.

    Returns:
        xarray.DataArray: The histogram of the input data.
    """

    if isinstance(data, xr.Dataset):
        data = data[list(data.data_vars.keys())[0]]
    elif not isinstance(data, xr.DataArray):
        raise TypeError('Input data must be an xarray DataArray or Dataset')

    logger = log_configure(log_level=loglevel, log_name='Histogram')

    if units is not None:
        data = convert_data_units(data, var=data.name, units=units, loglevel=loglevel)

    logger.info('Computing histogram with the following parameters: bins={}, range={}'.format(bins, range))

    if weighted:
        logger.debug('Using latitudinal weights')
        if 'lat' not in data.coords:
            raise ValueError("DataArray must have a 'lat' coordinate for weighted histogram.")
        weights = xr.ones_like(data)
        weights = weights * weights.lat
        weights = np.cos(np.radians(weights))
    else:
        weights = None

    if dask and isinstance(data.data, da.Array):
        logger.debug('Using Dask for histogram computation')
        hist, edges = da.histogram(data, weights=weights, bins=bins, range=range, density=density)
    else:
        logger.debug('Using NumPy for histogram computation')
        hist, edges = np.histogram(data, weights=weights, bins=bins, range=range, density=density)
    
    size_of_the_data = data.size

    if check and not density:
        if isinstance(hist, da.Array):
            hist = hist.compute()
        if int(sum(hist)) != size_of_the_data:
            logger.warning('Sum of counts in the histogram is not equal to the size of the data')

    center_of_bin = [ 0.5 * (edges[i] + edges[i+1]) for i in builtins.range(len(edges)-1)]
    width_table = [ edges[i+1] - edges[i] for i in builtins.range(len(edges)-1)]

    counts_per_bin = xr.DataArray(hist, coords=[center_of_bin], dims=["center_of_bin"])
    counts_per_bin = counts_per_bin.assign_coords(width=("center_of_bin", width_table))

    counts_per_bin.attrs = data.attrs

    counts_per_bin.center_of_bin.attrs['units'] = data.units

    counts_per_bin.attrs['size_of_the_data'] = size_of_the_data

    if density:
        if "long_name" in counts_per_bin.attrs:
            counts_per_bin.attrs['long_name'] = 'Pdf of {}'.format(counts_per_bin.attrs['long_name'])
        counts_per_bin.name = 'pdf'
        counts_per_bin.attrs['units'] = 'probability density'
    else:
        if "long_name" in counts_per_bin.attrs:
            counts_per_bin.attrs['long_name'] = 'Histogram of {}'.format(counts_per_bin.attrs['long_name'])
        counts_per_bin.name = 'histogram'
        counts_per_bin.attrs['units'] = 'counts'

    return counts_per_bin
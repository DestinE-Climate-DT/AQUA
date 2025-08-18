"""
Module to evaluate the confidence intervals of the teleconnections using bootstrapping.
"""
import numpy as np
import xarray as xr

from aqua.logger import log_configure
from aqua.diagnostics.teleconnections import NAO, ENSO

xr.set_options(keep_attrs=True)


def bootstrap_teleconnections(telec_model,
                              telec_ref,
                              var: str,
                              n_bootstraps: int = 1000,
                              concordance: float = 0.05,
                              statistic: str = None,
                              season: str = None,
                              loglevel: str = 'WARNING'):
    """
    Bootstrap the regression and correlation maps.

    Args:
        telec_model (NAO or ENSO): Teleconnection model instance for the dataset.
        telec_ref (NAO or ENSO): Teleconnection model instance for the reference dataset.
        n_bootstraps (int): Number of bootstraps to perform. Default is 1000.
        concordance (float): Concordance threshold. Default is 0.5.
        statistic (str): Statistic to compute. Default is None.
                         Available options are 'regression' and 'correlation'.
        season (str): Season to consider for the bootstrap. Default is None.
        loglevel (str): Logging level. Default is 'WARNING'.
        eval_kwargs (dict): Additional keyword arguments to pass to the evaluation function.

    Returns:
        xr.DataArray: Lower percentile map
        xr.DataArray: Upper percentile map
    """
    logger = log_configure(loglevel, 'Bootstrap teleconnections')

    if statistic != 'regression' and statistic != 'correlation':
        raise ValueError('Invalid statistic provided. Please provide a statistic to compute (reg or cor).')
    
    index_model = telec_model.index
    index_ref = telec_ref.index
    if var != telec_ref.var:
        logger.warning(f'Variable {var} does not match the reference teleconnection variable {telec_ref.var}. Retrieving {var}.')
        telec_ref.retrieve(var=var)
    data_ref = telec_ref.data

    # Build the bootstrap maps DataArray
    # bootstrap_maps = xr.DataArray(np.zeros((n_bootstraps,) + map.shape),
    #                               coords=[range(n_bootstraps)] + [coord for coord in map.coords.values()],
    #                               dims=['bootstrap'] + [dim for dim in map.dims])
    bootstrap_maps = xr.DataArray(np.zeros((n_bootstraps,) + data_ref[var].isel(time=0).shape),
                                  coords=[range(n_bootstraps)] + [coord for coord in data_ref[var].coords.values() if coord.name != 'time'],
                                  dims=['bootstrap'] + [dim for dim in data_ref[var].dims if dim != 'time'])

    # Bootstrap the maps
    for i in range(n_bootstraps):
        logger.info(f'Bootstrap {i+1}/{n_bootstraps}')

        boot_time = np.random.choice(index_ref.time.values, index_model.time.size, replace=True)

        boot_index = index_ref.sel(time=boot_time)
        boot_data = data_ref.sel(time=boot_time)

        telec_ref.index = boot_index  # Update the index for the reference teleconnection
        telec_ref.data = boot_data  # Update the data for the reference teleconnection
        if statistic == 'regression':
            bootstrap_maps.loc[dict(bootstrap=i)] = telec_ref.compute_regression(season=season)
        elif statistic == 'correlation':
            bootstrap_maps.loc[dict(bootstrap=i)] = telec_ref.compute_correlation(season=season)

    # Evaluate the percentile confidence intervals
    upper = bootstrap_maps.quantile(1-concordance, dim='bootstrap')
    lower = bootstrap_maps.quantile(concordance, dim='bootstrap')

    telec_ref.index = index_ref  # Restore the original index for the reference teleconnection
    telec_ref.data = data_ref  # Restore the original data for the reference teleconnection
    logger.info('Bootstrap confidence intervals computed.')

    return lower, upper


def build_confidence_mask(map: xr.DataArray, lower: xr.DataArray, upper: xr.DataArray,
                          mask_concordance=True):
    """
    Build the confidence masks based on the lower and upper percentiles.

    Args:
        reg (xr.DataArray): Regression map of the dataset
        lower (xr.DataArray): Lower percentile map
        upper (xr.DataArray): Upper percentile map
        mask_concordance (bool): Whether to mask the concordance regions. Default is True.

    Returns:
        xr.DataArray: Confidence mask
    """
    if mask_concordance:
        mask = xr.where((map > lower) & (map < upper), True, False)
    else:  # Mask the discordance regions
        mask = xr.where((map < lower) | (map > upper), True, False)

    return mask

# THIS IS A LEGACY CODE
# Alternative approach, done with the P2 method (RAJ JAIN 1985)
# It allows a step by step evaluation of the percentiles, but it is not
# adapted to dask.

# def bootstrap_teleconnections(map: xr.DataArray,
#                               index: xr.DataArray,
#                               index_ref: xr.DataArray,
#                               data_ref, # can be either a DataArray or a Dataset
#                               n_bootstraps=100,
#                               concordance=0.5,
#                               statistic=None,
#                               loglevel='WARNING',
#                               **eval_kwargs):
#     """
#     Bootstrap the regression and correlation maps.

#     Args:
#         reg (xr.DataArray): Regression map of the dataset
#         index (xr.DataArray): Index of the dataset
#         index_ref (xr.DataArray): Index of the reference dataset
#         data_ref (xr.DataArray): Data of the reference dataset to perform the regression
#                                  or correlation with.
#         n_bootstraps (int): Number of bootstraps to perform. Default is 1000.
#         concordance (float): Concordance threshold. Default is 0.5.
#         statistic (str): Statistic to compute. Default is None.
#                          Available options are 'reg' and 'cor'.
#         loglevel (str): Logging level. Default is 'WARNING'.
#         eval_kwargs (dict): Additional keyword arguments to pass to the evaluation function.
#     """
#     logger = log_configure(loglevel, 'Bootstrap teleconnections')

#     if statistic is None:
#         raise ValueError('No statistic was provided. Please provide a statistic to compute (reg or cor).')

#     if isinstance(data_ref, xr.Dataset):
#         data_ref = data_ref[list(data_ref.keys())[0]]

#     # Create empty arrays to store percentiles, dropping the time axis
#     lower = xr.DataArray(np.zeros([data_ref.shape[i] for i in range(len(data_ref.dims)) if data_ref.dims[i] != 'time']),
#                          coords={k: v for k, v in data_ref.coords.items() if k != 'time'},
#                          dims=[dim for dim in data_ref.dims if dim != 'time'])
#     upper = xr.DataArray(np.zeros([data_ref.shape[i] for i in range(len(data_ref.dims)) if data_ref.dims[i] != 'time']),
#                          coords={k: v for k, v in data_ref.coords.items() if k != 'time'},
#                          dims=[dim for dim in data_ref.dims if dim != 'time'])

#     # Initialize the P2 algorithm for each pixel
#     logger.info('Initializing P2 algorithm')
#     p2 = np.array([P2Algorithm() for _ in range(lower.size)])
#     logger.debug(f'Number of pixels: {lower.size}')

#     # Bootstrap the maps pixel by pixel
#     for i in range(n_bootstraps):
#         logger.debug(f'Bootstrap {i+1}/{n_bootstraps}')

#         boot_time = np.random.choice(index_ref.time.values, index.time.size, replace=True)

#         boot_index = index_ref.sel(time=boot_time)
#         boot_data = data_ref.sel(time=boot_time)

#         if statistic == 'reg':
#             bootstrap_values = reg_evaluation(indx=boot_index, data=boot_data, **eval_kwargs)
#         elif statistic == 'cor':
#             bootstrap_values = cor_evaluation(indx=boot_index, data=boot_data, **eval_kwargs)
#         logger.debug('Bootstrap map evaluated')
#         bootstrap_values.load()

#         # Update the P2 algorithm for each pixel
#         for j in range(bootstrap_values.size):
#             p2[j].add(bootstrap_values.values.flat[j])

#     # Compute the percentiles for each pixel
#     logger.info('Computing percentiles')
#     for j in range(lower.size):
#         lower.values.flat[j] = p2[j].get_percentile(1-concordance/2)
#         upper.values.flat[j] = p2[j].get_percentile(concordance/2)

#     return lower, upper


# class P2Algorithm:
#     def __init__(self):
#         self.n = 0
#         self.sorted_data = []

#     def add(self, x):
#         self.n += 1
#         if self.n == 1:
#             self.sorted_data.append(x)
#         else:
#             if x >= self.sorted_data[-1]:
#                 self.sorted_data.append(x)
#             else:
#                 self.sorted_data.insert(self.find_index(x), x)

#     def find_index(self, x):
#         for i, data_point in enumerate(self.sorted_data):
#             if x < data_point:
#                 return i
#         return 0

#     def get_percentile(self, p):
#         if not self.sorted_data:
#             return None
#         k = (self.n - 1) * p
#         f = int(k)
#         c = k - f
#         if f + 1 < len(self.sorted_data):
#             return self.sorted_data[f] + c * (self.sorted_data[f + 1] - self.sorted_data[f])
#         else:
#             return self.sorted_data[f]
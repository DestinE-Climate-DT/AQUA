"""Utility for the GlobalBiases module"""

import xarray as xr
from aqua.logger import log_configure
from aqua.exceptions import NoDataError

def handle_pressure_level(data, var, plev, loglevel='WARNING'):
    """
    Handles selection of a specific pressure level from the dataset.

    Args:
        data (xarray.Dataset): Dataset to select from.
        var (str): Variable name to filter by.
        plev (float, optional): Desired pressure level.
        loglevel (str): The logging level (default 'WARNING')
    Returns:
        xarray.Dataset or None: Dataset at specified pressure level, or None if skipped.
    """
    logger = log_configure(loglevel, 'Pressure levels')

    if var not in data:
        raise NoDataError(f"Variable '{var}' not found in the dataset.")

    # if the variable does not have a 'plev' dimension, return the data as is
    if 'plev' in data[var].coords:
        if plev is None:
            logger.warning(
                f"Variable '{var}' has multiple pressure levels, but no specific level was selected. ")
            return data  

        # if 'plev' has already a single value, check if it matches the requested plev
        if 'plev' in data[var].coords and data[var].coords['plev'].size == 1:
            if data[var].coords['plev'].values[0] == plev:
                return data

        # try to select the closest pressure level
        try:
            logger.info(f"Selecting pressure level {plev} for variable '{var}'.")
            return data.sel(plev=plev, method="nearest")
        except KeyError:
            raise NoDataError(f"The specified pressure level {plev} is not in the dataset.")

    # if the variable does not have a 'plev' dimension, raise and error if plev is specified
    elif plev is not None:
        raise ValueError(f"Variable '{var}' does not have a 'plev' dimension, but a pressure level was requested.")
    return data

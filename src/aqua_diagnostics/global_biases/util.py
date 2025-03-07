"""Utility for the GlobalBiases module"""

import xarray as xr
from aqua.exceptions import NoDataError

def fix_precipitation_units(data, var):
    """
    Converts precipitation units from kg m-2 s-1 to mm/day.

    Args:
        data (xr.Dataset): Dataset to adjust.
        var (str): Variable name for precipitation.

    Returns:
        xr.Dataset: Dataset with adjusted units.
    """
    if data[var].attrs['units'] != 'mm/day':
        data[var] *= 86400
        data[var].attrs['units'] = 'mm/day'
    return data


def select_pressure_level(data, plev, var):
    """
    Selects a specified pressure level from the dataset.

    Args:
        data (xr.Dataset): Dataset to select from.
        plev (float): Desired pressure level.
        var (str): Variable name to filter by.

    Returns:
        xr.Dataset: Filtered dataset at specified pressure level.

    Raises:
        NoDataError: If specified pressure level is not available.
    """
    if 'plev' in data[var].dims:
        try:
            return data.sel(plev=plev)
        except KeyError:
            raise NoDataError("The specified pressure level is not in the dataset.")
    else:
        raise NoDataError(f"{var} does not have a 'plev' coordinate.")

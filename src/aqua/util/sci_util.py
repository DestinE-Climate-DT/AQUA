"""Module for scientific utility functions."""
import xarray as xr
from aqua.logger import log_configure, log_history

# set default options for xarray
xr.set_options(keep_attrs=True)


def lon_to_360(lon: float) -> float:
    """
    Convert longitude from [-180,180] (or any value) to [0,360].

    Args:
        lon (float): longitude coordinate

    Returns:
        float: converted longitude
    """
    lon = lon % 360
    return 0.0 if lon == 360 else lon


def lon_to_180(lon: float) -> float:
    """
    Convert longitude from [0,360] (or any value) to [-180,180].

    Args:
        lon (float): longitude coordinate

    Returns:
        float: converted longitude
    """
    lon = lon % 360
    return lon - 360 if lon > 180 else lon


def select_season(xr_data, season: str):
    """
    Select a season from a xarray.DataArray or xarray.Dataset.
    Available seasons are:
    - DJF: December-January-February
    - JFM: January-February-March
    - FMA: February-March-April
    - MAM: March-April-May
    - AMJ: April-May-June
    - MJJ: May-June-July
    - JJA: June-July-August
    - JAS: July-August-September
    - ASO: August-September-October
    - SON: September-October-November
    - OND: October-November-December
    - NDJ: November-December-January
    Args:
        xr_data (xarray.DataArray or xarray.Dataset): input data
        season (str):                                 season to be selected
    Returns:
        (xarray.DataArray or xarray.Dataset): selected season
    """
    triplet_months = {
        'DJF': [12, 1, 2],
        'JFM': [1, 2, 3],
        'FMA': [2, 3, 4],
        'MAM': [3, 4, 5],
        'AMJ': [4, 5, 6],
        'MJJ': [5, 6, 7],
        'JJA': [6, 7, 8],
        'JAS': [7, 8, 9],
        'ASO': [8, 9, 10],
        'SON': [9, 10, 11],
        'OND': [10, 11, 12],
        'NDJ': [11, 12, 1]
    }

    if season in triplet_months:
        selected_months = triplet_months[season]
        selected =  xr_data.sel(time=(xr_data['time.month'] == selected_months[0]) | (xr_data['time.month'] == selected_months[1]) | (xr_data['time.month'] == selected_months[2]))
        # Add AQUA_season attribute
        selected.attrs['AQUA_season'] = season
        return selected
    elif season == 'annual':
        return xr_data
    else:
        raise ValueError(f"Invalid season abbreviation. Available options are: {', '.join(triplet_months.keys())}, or 'annual' to perform no season selection.")

def merge_attrs(target, source, overwrite=False):
    """Merge attributes from source into target.

    Args:
        target (xr.Dataset or xr.DataArray or dict): The target for merging.
        source (xr.Dataset or xr.DataArray or dict): The source of attributes.
        overwrite (bool): If True, overwrite existing keys in target.
                          If False, only add keys that don't already exist.
    """
    if isinstance(target, (xr.Dataset, xr.DataArray)):
        target = target.attrs
    if isinstance(source, (xr.Dataset, xr.DataArray)):
        source = source.attrs

    for k, v in source.items():
        if overwrite or k not in target:
            target[k] = v
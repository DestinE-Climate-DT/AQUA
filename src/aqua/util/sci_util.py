"""Module for scientific utility functions."""
import xarray as xr

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
    return 360 if lon == 360 else lon % 360


def lon_to_180(lon: float) -> float:
    """
    Convert longitude from [0,360] (or any value) to [-180,180].

    Args:
        lon (float): longitude coordinate

    Returns:
        float: converted longitude
    """
    if lon == -180:
        return -180
    lon = lon % 360
    return lon - 360 if lon > 180 else lon


def check_coordinates(lon: list | None, lat: list | None,
                           default_coords: dict) -> tuple[list, list]:
        """
        Validate and normalize latitude/longitude ranges.

        Returns:
            tuple: (lon_range, lat_range) with values mapped to default system.
        """
        # --- Latitude ---
        # Populate with maximum extent if no Latitude is provided
        if lat is None:
            lat = [default_coords["lat_min"], default_coords["lat_max"]]
        # Swap if values are inverted
        elif lat[0] > lat[1]:
            lat = [lat[1], lat[0]]
        # If the two latitudes are equal raise an error
        elif lat[0] == lat[1]:
            raise ValueError(f"Both latitude values are equal: {lat[0]}, please provide a valid range.")
        # Check that values are within the maximum range
        if lat[0] < default_coords["lat_min"] or lat[1] > default_coords["lat_max"]:
            raise ValueError(f"Latitude must be within {default_coords['lat_min']} and {default_coords['lat_max']}")

        # --- Longitude ---
        # Populate with maximum extent if no Longitude is provided
        if lon is None or (lon[0] == 0 and lon[1] == 360) or (lon[0] == -180 and lon[1] == 180):
            lon = [default_coords["lon_min"], default_coords["lon_max"]]
        # If the two longitudes are equal raise an error
        elif lon[0] == lon[1]:
            raise ValueError(f"Longitude: {lon[0]} == {lon[1]}, please provide a valid range.")
        else:
            # Normalize according to coordinate system
            if default_coords["lon_min"] == 0 and default_coords["lon_max"] == 360:
                lon = [lon_to_360(l) for l in lon]
            elif default_coords["lon_min"] == -180 and default_coords["lon_max"] == 180:
                lon = [lon_to_180(l) for l in lon]

        return lon, lat


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
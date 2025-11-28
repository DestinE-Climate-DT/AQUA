"""Module for scientific utility functions."""
import xarray as xr

# set default options for xarray
xr.set_options(keep_attrs=True)


TRIPLET_MONTHS = {
    'DJF': [12, 1, 2],   # December-January-February
    'JFM': [1, 2, 3],    # January-February-March
    'FMA': [2, 3, 4],    # February-March-April
    'MAM': [3, 4, 5],    # March-April-May
    'AMJ': [4, 5, 6],    # April-May-June
    'MJJ': [5, 6, 7],    # May-June-July
    'JJA': [6, 7, 8],    # June-July-August
    'JAS': [7, 8, 9],    # July-August-September
    'ASO': [8, 9, 10],   # August-September-October
    'SON': [9, 10, 11],  # September-October-November
    'OND': [10, 11, 12], # October-November-December
    'NDJ': [11, 12, 1],  # November-December-January
}

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

    Args:
        xr_data (xarray.DataArray or xarray.Dataset): input data
        season (str): season to be selected. Available options are defined in TRIPLET_MONTHS.
    Returns:
        (xarray.DataArray or xarray.Dataset): selected season
    """
    if season in TRIPLET_MONTHS:
        selected_months = TRIPLET_MONTHS[season]
        selected =  xr_data.sel(time=(xr_data['time.month'] == selected_months[0]) | (xr_data['time.month'] == selected_months[1]) | (xr_data['time.month'] == selected_months[2]))
        # Add AQUA_season attribute
        selected.attrs['AQUA_season'] = season
        return selected
    elif season == 'annual':
        return xr_data
    else:
        raise ValueError(f"Invalid season abbreviation. Available options are: {', '.join(TRIPLET_MONTHS.keys())}, or 'annual' to perform no season selection.")


def generate_quarter_months(anchor_month='JAN'):
    """
    Construct four consecutive quarters every 3rd triplet starting from the anchor month.
    Args:
        anchor_month (str): The anchor month for the quarterly groupings.
    Returns:
        dict: A dictionary of quarterly month groupings.
    """
    anchor_month = anchor_month.upper()

    monlist = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    triplet_keys = list(TRIPLET_MONTHS)

    if anchor_month not in monlist:
        raise ValueError(f"Invalid anchor month: {anchor_month}. Must be one of: {', '.join(monlist)}")

    anchor_month_num = monlist.index(anchor_month) + 1

    start_idx_anchor_month = next(i for i, key in enumerate(triplet_keys) 
                                  if TRIPLET_MONTHS[key][0] == anchor_month_num)

    quarter_months = {f"Q{q + 1}": TRIPLET_MONTHS[triplet_keys[(start_idx_anchor_month + q * 3) % len(triplet_keys)]] for q in range(4)}

    return {anchor_month: quarter_months}
    

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


def find_vert_coord(ds):
    """
    Identify the vertical coordinate name(s) based on coordinate units. Returns always a list.
    The list will be empty if none found.
    """
    vert_coord = [x for x in ds.coords if ds.coords[x].attrs.get("units") in ["Pa", "hPa", "m", "km", "Km", "cm", ""]]
    return vert_coord

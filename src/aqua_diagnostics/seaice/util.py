"""Utility for the sea ice plotting module"""

import os
import xarray as xr
from aqua.logger import log_configure
from collections import defaultdict
from aqua.util import load_yaml
from aqua.configurer import ConfigPath

def defaultdict_to_dict(d):
    """ Recursively converts a defaultdict to a normal dict."""
    if isinstance(d, defaultdict):
        return {k: defaultdict_to_dict(v) for k, v in d.items()}
    return d



def filter_region_list(regions_dict, regions_list, domain, logger, valid_domains=None):
    """ Filters a list of string regions based on config_file defined coords values and specified domain.
    This function checks if regions fall within the appropriate hemisphere based on their latitude bounds.

    Args:
        regions_dict (dict): Dictionary containing region definitions.
        regions_list (list): List of region names to be filtered.
        domain (str): Domain to filter regions by. Must be one of the valid domains, e.g., 'nh' or 'sh'.
        logger (logging.Logger): Logger instance for logging messages.
        valid_domains (list, optional): List of valid domain strings. Defaults to ['nh', 'sh'].
    
    Returns:
        list or str: Filtered list of region names. If exactly one region is valid, returns a single string.
    """
    if not valid_domains:
        valid_domains = ['nh', 'sh']

    if domain not in valid_domains:
        raise ValueError(f"Invalid domain '{domain}'. Valid domains are: {valid_domains}")
    
    filtered_regions = []

    for r in regions_list:
        if r in regions_dict['regions'].keys():
            region_info = regions_dict['regions'][r]
            lat_limits = region_info.get('lat_limits', [])

            if len(lat_limits) >= 2:
                min_lat, max_lat = lat_limits[0], lat_limits[1]

            if domain == 'nh' and min_lat >= 0:   # Northern hemisphere
                filtered_regions.append(r)
            elif domain == 'sh' and max_lat <= 0: # Southern hemisphere
                filtered_regions.append(r)
            else:
                logger.debug(f"Region '{r}' doesn't meet the data domain criteria for {domain}, not including in regions_list.")
        else:
            logger.error(f"No region '{r}' defined in regions_dict from yaml. Check this mismatch.")
            
    return filtered_regions

def ensure_istype(obj, expected_types, logger=None):
    """ Ensure an object is of the expected type(s), otherwise raise ValueError.

    Args:
        obj: The object to check.
        expected_types: A type or tuple of types to check against.
        logger (optional): Logger for reporting the error, if provided.
    """
    if isinstance(expected_types, tuple):
        expected_names_type = ", ".join(t.__name__ for t in expected_types)
    else:
        expected_names_type = expected_types.__name__

    if not isinstance(obj, expected_types):
        raise ValueError(f"Expected type {expected_names_type}, but got {type(obj).__name__}.")

def extract_dates(data):
    """
    Extracts start and end dates from data attributes.
    If the date is a datetime object, it is formatted as 'YYYY-MM-DD'.
    If the date is a string, it is returned as is.
    """
    def fmt_dt(attr_name):
        dt = data.attrs.get(attr_name, f'No {attr_name} found')
        if hasattr(dt, 'strftime'): return dt.strftime('%Y-%m-%d')
        if isinstance(dt, str) and 'T' in dt: return dt.split('T')[0]
        return dt
    return fmt_dt('AQUA_startdate'), fmt_dt('AQUA_enddate')

def _check_list_regions_type(regions_to_plot, logger=None):
    """Ensures regions_to_plot is a list of strings before assigning it."""
    if regions_to_plot is None:
        logger.warning("Expected regions_to_plot to be a list, but got None. Plotting all available regions in data.")
        return None

    if not isinstance(regions_to_plot, list):
        raise TypeError(  f"Expected regions_to_plot to be a list, but got {type(regions_to_plot).__name__}.")
    
    if not all(isinstance(region, str) for region in regions_to_plot):
        invalid_types = [type(region).__name__ for region in regions_to_plot]
        raise TypeError(  f"Expected a list of strings, but found element types: {invalid_types}.")
    return regions_to_plot
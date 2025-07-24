"""Utility for the sea ice plotting module"""

import os
import xarray as xr
from aqua.logger import log_configure
from collections import defaultdict
from aqua.util import load_yaml, ConfigPath

def defaultdict_to_dict(d):
    """ Recursively converts a defaultdict to a normal dict."""
    if isinstance(d, defaultdict):
        return {k: defaultdict_to_dict(v) for k, v in d.items()}
    return d

def filter_region_list(regions_dict, regions_list, domain, logger, valid_domains=None):
    """ Filters a list of string regions based on config_file defined coords values and specified domain.
    This function checks if regions fall within the appropriate hemisphere based on their latitude bounds.

    Args:
        regions_dict (dict): Dictionary containing region definitions. Each region should have
            'latN' (northern latitude) and 'latS' (southern latitude) keys.
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
        logger.error(f"Invalid domain '{domain}'. Expected {valid_domains}.")
    
    filtered_regions = []

    for r in regions_list:
        if r in regions_dict.keys():
            latN = regions_dict[r].get('latN')
            latS = regions_dict[r].get('latS')

            if domain == 'nh' and latN is not None and latN > 0:
                filtered_regions.append(r)
            elif domain == 'sh' and latS is not None and latS < 0:
                filtered_regions.append(r)
            else:
                logger.info(f"Region '{r}' does not meet the data domain criteria for {domain}, so removing it from regions_list.")
        else:
            logger.error(f"No region '{r}' defined in regions_dict from yaml. Check this mismatch.")

    if len(filtered_regions) == 1:
        return filtered_regions[0]

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
        errmsg = f"Expected type {expected_names_type}, but got {type(obj).__name__}."
        if logger:
            logger.error(errmsg)
        raise ValueError(errmsg)

def extract_dates(data):
    return (data.attrs.get('AQUA_startdate', 'No startdate found'),
            data.attrs.get('AQUA_enddate',   'No enddate found'))

def strlist_to_phrase(items: list[str]) -> str:
    """ Convert a list of str to a english-consistent list.
       ['A'] will return 'A'
       ['A','B'] will return 'A and B'
       ['A','B','C'] will return 'A, B, and C'
    """
    if not items:
        return ""
    if len(items) == 1:
        return items[0]
    if len(items) == 2:
        return f"{items[0]} and {items[1]}"
    return ", ".join(items[:-1]) + f", and {items[-1]}"

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
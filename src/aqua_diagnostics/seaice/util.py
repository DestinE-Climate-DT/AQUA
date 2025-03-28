"""Utility for the sea ice plotting module"""

from aqua.logger import log_configure
from collections import defaultdict

def defaultdict_to_dict(d):
    """Recursively converts a defaultdict to a normal dict."""
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
            

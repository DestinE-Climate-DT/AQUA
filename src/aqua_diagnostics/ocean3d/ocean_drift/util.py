import xarray as xr
import numpy as np
from aqua.logger import log_configure
from itertools import product
from aqua.util import find_vert_coord, load_yaml, add_pdf_metadata
import os


def process_region_config(regions_yaml):
    """
    Processes the region configuration dictionary by normalizing the keys.

    Args:
        regions_yaml (dict): A dictionary containing region configurations.

    Returns:
        dict: A dictionary with keys converted to lowercase and with spaces, underscores, and dashes removed.
    """
    if isinstance(regions_yaml, dict):
        return {
            key.replace(" ", "").replace("_", "").replace("-", "").lower(): v
            for key, v in regions_yaml.items()
        }
    else:
        return regions_yaml


def predefined_regions(region, loglevel="WARNING"):
    """
    Retrieves the geographical boundaries (latitude and longitude) for a predefined region 
    from a configuration file.

    Args:
        region (str): The name of the region to retrieve. The name is processed to be 
            case-insensitive and ignores spaces, underscores, and hyphens.
        loglevel (str, optional): The logging level to use. Defaults to "WARNING".

    Returns:
        tuple: A tuple containing the southern latitude (lat_s), northern latitude (lat_n), 
            western longitude (lon_w), and eastern longitude (lon_e) of the region.

    Raises:
        ValueError: If the specified region is not found in the configuration file.

    Notes:
        - The configuration file is expected to be located at 
          `../../../../config/diagnostics/ocean3d/regions.yaml` relative to this script.
        - The configuration file should contain a dictionary with a "regions" key, where 
          each region is defined with its boundaries (LatN, LatS, LonE, LonW).
    """
    logger = log_configure(loglevel, "predefined_regions")
    
    Configurer = ConfigPath()
    oceandir = os.path.join(Configurer.configdir, 'diagnostics', 'ocean3d')
    regionfile =  os.path.join(oceandir, 'config', 'regions.yaml')
    regions_dict = load_yaml(regionfile)
            
    processed_region = region.replace(" ", "").replace("_", "").replace("-", "").lower()

    try:
        regions_dict = process_region_config(regions_dict["regions"])
        region_boundary = regions_dict[processed_region]
        lon_limits = region_boundary.get("lon_limits")
        lat_limits = region_boundary.get("lat_limits")
    except KeyError:
        raise ValueError(
            f"Invalid region name: {region}. Check the region name in config file or update it: {regions_yaml}"
        )

    return lon_limits, lat_limits



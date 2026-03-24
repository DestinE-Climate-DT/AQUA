"""Utility functions for coordinate handling and data model loading."""

from functools import cache
import os
from metpy.units import units
from pint.errors import DimensionalityError, UndefinedUnitError
from aqua.core.configurer import ConfigPath
from aqua.core.util import load_yaml

# Define the target dimensionality (pressure)
pressure_dim = units.pascal.dimensionality
meter_dim = units.meter.dimensionality

# module logger
# logger = log_configure(log_level='INFO', log_name='coord_utils')

# Possible basic names for coordinates
DEFAULT_COORD_NAMES = {
    "latitude": [
        "latitude",
        "lat",
    ],
    "longitude": [
        "longitude", 
        "lon"
    ],
    "time": ["time", "time_counter"],
    "isobaric": ["plev"],
    "depth": ["depth"],
    "height": ["height"],
}


@cache
def _load_coord_config():
    """
    Load coordinate configuration from YAML file (cached once).
    """
    data_model_dir = os.path.join(ConfigPath().get_config_dir(), "data_model")
    config_path = os.path.join(data_model_dir, "coords_default.yaml")

    try:
        return load_yaml(config_path)
    except FileNotFoundError:
        # logger.warning("Failed to load config from %s. Using defaults. Error: %s", config_path, e)
        return DEFAULT_COORD_NAMES


def get_coord_defaults(internal_coord=None):
    """
    Get default coordinate names from cached config.

    Args:
        internal_coord (str): Coordinate type ('latitude', 'longitude', etc.)
                             If None, returns entire config dict.

    Returns:
        list or dict: List of names for the coordinate, or full config dict if internal_coord is None.
    """
    config = _load_coord_config()
    return config.get(internal_coord, DEFAULT_COORD_NAMES.get(internal_coord, []))


@cache
def _load_data_model(name: str = "aqua"):
    """
    Load the default data model from the aqua.yaml file.

    Args:
        name (str): An installed data_model into aqua config, i.e. a YAML file

    Returns:
        dict: Target coordinates dictionary.
        str: Name of the target data model.
    """
    data_model_dir = os.path.join(ConfigPath().get_config_dir(), "data_model")
    data_model_file = os.path.join(data_model_dir, f"{name}.yaml")
    if not os.path.exists(data_model_file):
        raise FileNotFoundError(f"Data model file {data_model_file} not found.")
    # logger.info("Loading data model from %s", data_model_file)
    data_yaml = load_yaml(data_model_file)
    return data_yaml


def get_data_model(name: str = "aqua"):
    """
    Get the default data model from the aqua.yaml file.

    Args:
        name (str): An installed data_model into aqua config, i.e. a YAML file
    """
    return _load_data_model(name)


# Function to get the conversion factor
def units_conversion_factor(from_unit_str, to_unit_str):
    """
    Get the conversion factor between two units.
    """
    try:
        from_unit = units(from_unit_str)
        to_unit = units(to_unit_str)
    except (UndefinedUnitError, AttributeError):
        return None

    try:
        return from_unit.to(to_unit).magnitude
    except (DimensionalityError, AttributeError):
        return None


def is_pressure(unit):
    """Check if a unit is a pressure unit."""
    try:
        return units(unit).dimensionality == pressure_dim
    except (UndefinedUnitError, AttributeError):
        return False


def is_meter(unit):
    """Check if a unit is a length unit (depth)."""
    try:
        return units(unit).dimensionality == meter_dim
    except (UndefinedUnitError, AttributeError):
        return False

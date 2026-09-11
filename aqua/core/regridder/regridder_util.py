"""Regridding utilities."""

import os

from aqua.core.default import DEFAULT_DIMENSION


def check_existing_file(filename):
    """
    Checks if an area/weights file exists and is valid.
    Return true if the file has some records.
    """
    return os.path.exists(filename) and os.path.getsize(filename) > 0


def get_grid_path(grid_path):
    """Get the grid path, looking for DEFAULT_DIMENSION or falling back to the first value."""
    return grid_path.get(DEFAULT_DIMENSION, next(iter(grid_path.values()), None))

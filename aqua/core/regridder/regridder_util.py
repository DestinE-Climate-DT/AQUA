"""Regridding utilities."""

import os


def check_existing_file(filename):
    """
    Checks if an area/weights file exists and is valid.
    Return true if the file has some records.
    """
    return os.path.exists(filename) and os.path.getsize(filename) > 0

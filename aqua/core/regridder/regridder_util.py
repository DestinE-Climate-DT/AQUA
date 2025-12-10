"""Regridding utilities."""

import os

def check_existing_file(filename):
    """
    Checks if an area/weights file exists and is valid.
    Return true if the file has some records.
    """
    return os.path.exists(filename) and os.path.getsize(filename) > 0


def validate_reader_kwargs(reader_kwargs):
    """
    Validate the reader kwargs.
    """
    if not reader_kwargs:
        raise ValueError("reader_kwargs must be provided.")
    for key in ["model", "exp", "source"]:
        if key not in reader_kwargs:
            raise ValueError(f"reader_kwargs must contain key '{key}'.")
    return reader_kwargs

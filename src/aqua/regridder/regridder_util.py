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


def configure_masked_fields(src_grid_dict):
    """
    if the grids has the 'masked' option, this can be based on
    generic attribute or alternatively of a series of specific variables using the 'vars' key

    Args:
        source_grid (dict): Dictionary containing the grid information

    Returns:
        masked_attr (dict): Dict with name and proprierty of the attribute to be used for masking
        masked_vars (list): List of variables to mask
    """
    masked_info = src_grid_dict.get("masked")
    if masked_info is None:
        return None, None

    masked_vars = masked_info.get("vars")
    masked_attrs = {k: v for k, v in masked_info.items() if k !=
                    "vars"} or None

    return masked_attrs, masked_vars

"""Module containing general utility functions for AQUA"""

from __future__ import annotations

import os
import sys

import xarray as xr


def to_list(arg):
    """
    Converts the input to a list.
    - Returns [] if input is None.
    - Returns the list itself if input is already a list.
    - Converts tuples, sets, and dictionaries to a list.
    - Wraps other types in a single-element list.

    Parameters:
    arg: The input object to convert.

    Returns:
    list: A list representation of the input.
    """
    if arg is None:  # Preserve None
        return []
    if isinstance(arg, list):  # Already a list
        return arg
    if isinstance(arg, (tuple, set)):  # Convert tuples and sets to a list
        return list(arg)
    if isinstance(arg, dict):  # Convert dictionary keys to a list
        return list(arg.keys())
    return [arg]


def get_arg(args, arg, default):
    """
    Support function to get arguments

    Args:
        args: the arguments
        arg: the argument to get
        default: the default value

    Returns:
        The argument value or the default value
    """
    return getattr(args, arg, None) or default


def check_attrs(da, att) -> bool:
    """
    Check if a DataArray or Dataset has a specific attribute.

    Arguments:
        da (xarray.DataArray or xarray.Dataset): Object to check
        att (dict or str): Attribute to check for

    Returns:
        Boolean
    """
    if not att:
        return False
    if isinstance(att, str):
        return att in da.attrs
    if isinstance(att, dict):
        key = next(iter(att))
        return da.attrs.get(key) == att[key]
    return False


def set_attrs(ds, attrs) -> xr.Dataset | xr.DataArray:
    """
    Set an attribute for all variables in an xarray.Dataset

    Args:
        ds (xarray.Dataset or xarray.DataArray): Dataset to set attributes on
        attrs (dict): Dictionary of attributes to set

    Returns:
        xarray.Dataset or xarray.DataArray: Updated Dataset or DataArray, or the same object if not this.
    """
    if not isinstance(attrs, dict):
        raise TypeError("The 'attrs' argument must be a dictionary.")

    if isinstance(ds, xr.Dataset):
        for var in ds.data_vars:
            ds[var].attrs.update(attrs)
    elif isinstance(ds, xr.DataArray):
        ds.attrs.update(attrs)
    return ds


def extract_attrs(data, attr) -> list | None:
    """Extract attribute(s) from dataset or list of datasets.
    Args:
        data (xarray.Dataset or list of xarray.Dataset): Dataset(s) to extract
        attr (str): Attribute name to extract.
        Returns:
            list | None: List of attribute values from the dataset(s).
    """
    if data is None:
        return None
    if isinstance(data, list):
        return [getattr(ds, attr, None) for ds in data]
    return getattr(data, attr, None)


class HiddenPrints:
    # from stackoverflow https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print#:~:text=If%20you%20don't%20want,the%20top%20of%20the%20file. # noqa
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, "w")

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout


def expand_env_vars(obj):
    """
    Recursively apply os.path.expandvars to all strings in a nested structure.
    Works for dicts, lists, and strings.
    """
    if isinstance(obj, dict):
        return {k: expand_env_vars(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [expand_env_vars(v) for v in obj]
    if isinstance(obj, str):
        return os.path.expandvars(obj)
    return obj

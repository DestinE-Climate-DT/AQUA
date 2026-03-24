"""Module containing general utility functions for AQUA"""

from __future__ import annotations
import os
import sys


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

    res = getattr(args, arg)
    if not res:
        res = default
    return res


def extract_attrs(data, attr):
    """Extract attribute(s) from dataset or list of datasets.
    Args:
        data (xarray.Dataset or list of xarray.Dataset): Dataset(s) to extract
        attr (str): Attribute name to extract.
        Returns:
            list: List of attribute values from the dataset(s).
    """
    if data is None:
        return None
    if isinstance(data, list):
        return [getattr(ds, attr, None) for ds in data]
    return getattr(data, attr, None)


def username():
    """
    Retrieves the current user's username from the 'USER' environment variable.
    """
    user = os.getenv('USER')
    if user is None:
        raise EnvironmentError("The 'USER' environment variable is not set.")
    return user

class HiddenPrints:
    # from stackoverflow https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print#:~:text=If%20you%20don't%20want,the%20top%20of%20the%20file. # noqa
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

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
  elif isinstance(obj, list):
    return [expand_env_vars(v) for v in obj]
  elif isinstance(obj, str):
    return os.path.expandvars(obj)
  else:
    return obj
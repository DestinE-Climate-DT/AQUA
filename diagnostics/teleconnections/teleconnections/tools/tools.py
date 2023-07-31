'''
This module contains miscellaneous tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
'''
import xarray as xr

from aqua.util import load_yaml, get_config_dir


def load_namelist(diagname='teleconnections', configdir=None):
    """
    Load diagnostic yaml file.

    Args:
        diagname (str, opt):    diagnostic name. Default is 'teleconnections'
        configdir (str, opt):   path to config directory. Default is Nones

    Returns:
        (dict):        Diagnostic configuration
    """
    if configdir is None:
        filename = f'{diagname}.yaml'
        configdir = get_config_dir(filename)

    infile = f'{configdir}/{diagname}.yaml'
    namelist = load_yaml(infile)

    return namelist


def _check_dim(data: xr.DataArray, dim: str):
    """
    Check if dimension is in data.

    Args:
        data (DataArray):   DataArray
        dim (str):          Dimension

    Raises:
        ValueError:         If dimension is not in data
    """
    if dim not in data.dims:
        raise ValueError(f'{dim} not in {data.dims}')

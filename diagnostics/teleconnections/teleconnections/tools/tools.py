'''
This module contains miscellaneous tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
'''
import os
import xarray as xr

from aqua.util import load_yaml, ConfigPath


class TeleconnectionsConfig(ConfigPath):
    """
    Class to handle the configuration of the teleconnections diagnostic.

    Inherited from ConfigPath, which is a class to handle the configuration
    of the AQUA framework.
    """
    def __init__(self, configdir=None, diagname='teleconnections'):
        """
        Initialize the TeleconnectionsConfig class.

        Args:
            configdir (str, opt):   path to config directory. Default is None
            diagname (str, opt):    diagnostic name. Default is 'teleconnections'
        """
        self.filename = diagname + '.yaml'

        if not configdir:
            self.configdir = super().get_config_dir()
        else:
            self.configdir = configdir
        self.config_file = os.path.join(self.configdir, self.filename)

    def load_namelist(self):
        """
        Load diagnostic yaml file.

        Returns:
            (dict):        Diagnostic configuration
        """
        infile = f'{self.configdir}/{self.filename}'
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

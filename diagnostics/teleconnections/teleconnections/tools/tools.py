'''
This module contains miscellaneous tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
'''
import os
import xarray as xr

from aqua.util import load_yaml


class TeleconnectionsConfig():
    """
    Class to handle the configuration of the teleconnections diagnostic.
    """
    def __init__(self, configdir=None, interface='teleconnections-destine'):
        """
        Initialize the TeleconnectionsConfig class.

        Args:
            configdir (str, opt):   path to config directory. Default is None
            interface (str, opt):    interface filename. Default is 'teleconnections-destine'
        """
        if interface:
            self.filename = interface + '.yaml'
        else:
            raise ValueError('No interface file specified')

        if not configdir:
            self.configdir = self.get_config_dir()
        else:
            self.configdir = configdir
        self.config_file = os.path.join(self.configdir, self.filename)

    def get_config_dir(self):
        """
        Return the path to the configuration directory,
        searching in a list of pre-defined directories.

        Returns:
            configdir (str): the dir of the teleconnections file and other config files

        Raises:
            FileNotFoundError: if no config file is found in the predefined folders
        """

        configdirs = []

        # if AQUA is defined
        aquadir = os.environ.get('AQUA')
        if aquadir:
            configdirs.append(os.path.join(aquadir, 'diagnostics',
                                           'teleconnections', 'config'))

        # set of predefined folders to browse
        configdirs.extend(['./config', '../config', '../../config',
                           '../../../config'])
        configdirs.extend(['./diagnostics/teleconnections/config',
                           '../diagnostics/teleconnections/config',
                           '../../diagnostics/teleconnections/config',
                           '../../../diagnostics/teleconnections/config'])

        for configdir in configdirs:
            if os.path.exists(os.path.join(configdir, self.filename)):
                return configdir

        raise FileNotFoundError(f"No config file {self.filename} found in {configdirs}")

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

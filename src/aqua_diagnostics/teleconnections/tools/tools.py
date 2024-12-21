'''
This module contains miscellaneous tools for the teleconnections diagnostic.
- loading functions, to deal with yaml files
'''
import os
import xarray as xr

from aqua.util import load_yaml, ConfigPath


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
            self.configdir = ConfigPath().get_config_dir()
        else:
            self.configdir = configdir

        self.config_file = os.path.join(self.configdir, 'diagnostics',
                                        'teleconnections', 'interface', self.filename)

    def load_namelist(self):
        """
        Load diagnostic yaml file.

        Returns:
            (dict):        Diagnostic configuration
        """
        namelist = load_yaml(self.config_file)

        return namelist


def check_dim(data, dim: str):
    """
    Check if dimension is in data.

    Args:
        data:   DataArray
        dim (str):          Dimension

    Raises:
        ValueError:         If dimension is not in data
    """
    if isinstance(data, xr.Dataset):
        data = data[list(data.keys())[0]]

    if dim not in data.dims:
        raise ValueError(f'{dim} not in {data.dims}')

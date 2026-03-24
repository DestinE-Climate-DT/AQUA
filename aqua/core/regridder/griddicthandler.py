"""Basic grid dictionary handling."""

from smmregrid import CdoGrid
from aqua.core.logger import log_configure
from .regridder_util import check_existing_file


class GridDictHandler:
    """
    Class to handle AQUA grid dictionaries.
    It is used to normalize all the different dictionaries and strings 
    that can be used to define a grid.
    It covers many cases (it might seem overdetailed), and can be used also 
    in absence of a cfg_grid_dict dictionary.
    """

    def __init__(self, cfg_grid_dict, default_dimension='2d', loglevel='WARNING'):

        """"
        Initialize the GridDictHandler.
        
        Args:
            cfg_grid_dict (dict): The AQUA grid dictionary from the configuration file.
                                  It can be None or empty dictionary but in this case 
                                  only CDO grid names can be used.
            default_dimension (str): The default dimension to use for the grid path. 
                                     '2d' by default.
            loglevel (str): The logging level.
        """

        self.cfg_grid_dict = cfg_grid_dict
        self.default_dimension = default_dimension
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='Regridder')

        # case for no grid dictionary provided
        if not self.cfg_grid_dict:
            self.logger.warning("No cfg_grid_dict dictionary provided, only CDO grid names can be used.")
            return

        # safety checks
        if not isinstance(self.cfg_grid_dict, dict):
            raise ValueError("cfg_grid_dict must be a dictionary.")
        if self.cfg_grid_dict.get('grids') is None:
            raise ValueError("No 'grids' key found in the cfg_grid_dict.")


    def normalize_grid_dict(self, grid_name):
        """
        Normalize the grid name to a grid dictionary, setting grid path as well.

        Args:
            grid_name (str): The grid name (could be a CDO grid).

        Returns:
            dict: The normalized AQUA grid dictionary. 
                  If grid name is none, dictionary with 'path' key as empty dictionary. 
        """

        grid_dict = self._normalize_grid_dict(grid_name)
        grid_dict['path'] = self._normalize_grid_path(grid_dict)
        return grid_dict

    def _normalize_grid_dict(self, grid_name):
        """
        Validate the grid name and return the grid dictionary.
        4 cases handled:
            - None, return an empty dictionary
            - a valid CDO grid name
            - a grid name in the configuration, again a valid CDO grid name
            - a grid dictionary in the configuration

        Args:
            grid_name (str): The grid name.

        Returns:
            dict: The normalized grid dictionary.
        """

        # if empty, return an empty dictionary
        if grid_name is None:
            return {}

        # error if it is not a string or a dictionary
        if not isinstance(grid_name, (str, dict)):
            raise TypeError(f"Grid name '{grid_name}' is not a valid type. str or dict expected.")

        # if a grid name is a valid CDO grid name, return it in the format of a dictionary
        if isinstance(grid_name, str) and CdoGrid(grid_name).grid_kind :
            self.logger.debug("Grid name %s is a valid CDO grid name.", grid_name)
            return {"path": {self.default_dimension: grid_name}}

        # stop if the grid dictionary is not provided
        if self.cfg_grid_dict is None:
            raise ValueError("No cfg_grid_dict dictionary provided, only CDO grid names can be used.")

        # if the grid name is not in the grids
        grid_dict = self.cfg_grid_dict['grids'].get(grid_name)
        if grid_dict is None:
            raise ValueError(f"Grid name '{grid_name}' not found in the configuration.")

        # grid dict is a string: this is the case of a CDO grid name
        if isinstance(grid_dict, str):
            if CdoGrid(grid_dict).grid_kind:
                self.logger.debug("Grid definition %s is a valid CDO grid name.", grid_dict)
                return {"path": {self.default_dimension: grid_dict}}
            raise ValueError(f"Grid name '{grid_dict}' is a string but not a valid CDO grid name.")
        # if the grid dict is a dictionary, return it
        return grid_dict

    def _normalize_grid_path(self, grid_dict):
        """
        Normalize the grid path to a dictionary with the self.default_dimension key.
        3 cases handled: 
            - an empty dictionary, return an empty dictionary
            - a dictionary with a path string, a CDO grid name or a file path
            - a dictionary with a dictionary of path, one for each vertical coordinate

        Args:
            path (str, dict): The grid path.
            data (xarray.Dataset): The dataset to extract grid information from.

        Returns:
            dict: The normalized grid path dictionary. "self.default_dimension" key is mandatory.
        """

        # if empty, return an empty dictionary
        path = grid_dict.get('path')
        if path is None:
            return {}

        # case path is a string: check if it is a valid CDO grid name or a file path
        if isinstance(path, str):
            if CdoGrid(path).grid_kind:
                self.logger.debug("Grid path %s is a valid CDO grid name.", path)
                return {self.default_dimension: path}
            if check_existing_file(path):
                self.logger.debug("Grid path %s is a valid file path.", path)
                return {self.default_dimension: path}
            raise FileNotFoundError(f"Grid file '{path}' does not exist.")

        # case path is a dictionary: check if the values are valid file paths
        # (could extend to CDO names?)
        if isinstance(path, dict):
            for _, value in path.items():
                if not (CdoGrid(value).grid_kind or check_existing_file(value)):
                    raise ValueError(f"Grid path '{value}' is not a valid CDO grid name nor a file path.")
            self.logger.debug("Grid path %s is a valid dictionary of file paths.", path)
            return path

        raise TypeError(f"Grid path '{path}' is not a valid type.")

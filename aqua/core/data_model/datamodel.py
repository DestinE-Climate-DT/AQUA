"""
DataModel class for applying base coordinate transformations.
Provides a clean interface to CoordTransformer with caching.
"""
import xarray as xr
import os
from functools import cache
from aqua.core.logger import log_configure
from aqua.core.data_model import CoordTransformer
from aqua.core.configurer import ConfigPath
from aqua.core.util import load_yaml


@cache
def _load_data_model(name: str = "aqua"):
    """
    Load the data model configuration from YAML file (cached).

    Args:
        name (str): Name of the data model file (e.g., "aqua", "cmip6")

    Returns:
        dict: Data model configuration dictionary
    """
    data_model_dir = os.path.join(ConfigPath().get_config_dir(), "data_model")
    data_model_file = os.path.join(data_model_dir, f"{name}.yaml")
    if not os.path.exists(data_model_file):
        raise FileNotFoundError(f"Data model file {data_model_file} not found.")
    return load_yaml(data_model_file)


class DataModel:
    """
    Manage base data model transformations.
    
    Provides a clean interface to apply standard coordinate transformations
    based on AQUA data model specifications. Works independently from fixes files.
    
    This class handles:
    - Coordinate identification (via CoordIdentifier)
    - Standard coordinate transformations (rename, units, direction)
    - Dimension alignment
    - Attribute standardization
    
    Args:
        name (str): Data model name (e.g., "aqua", "cmip6"). Default is "aqua".
        loglevel (str): Log level for logging. Default is 'WARNING'.
        
    Example:
        >>> datamodel = DataModel(name="aqua", loglevel="DEBUG")
        >>> data = datamodel.apply(data)
    """
    
    def __init__(self, name: str = "aqua", loglevel: str = 'WARNING'):
        """
        Initialize DataModel.
        
        Args:
            name (str): Data model name
            loglevel (str): Log level
        """
        self.name = name
        self.loglevel = loglevel
        self.logger = log_configure(log_level=loglevel, log_name='DataModel')
        
        # Load data model config (cached)
        self.logger.debug(f"Initializing DataModel: {self.name}")
        self.config = _load_data_model(self.name)
    
    def apply(self, data: xr.Dataset) -> xr.Dataset:
        """
        Apply base data model transformations to dataset.
        
        Args:
            data (xr.Dataset): Input dataset
            
        Returns:
            xr.Dataset: Transformed dataset with standardized coordinates
        """
        self.logger.info(f"Applying data model: {self.name}")
        return CoordTransformer(data, loglevel=self.loglevel).transform_coords(name=self.name)
    
    def get_config(self) -> dict:
        """
        Get the data model configuration.
        
        Returns:
            dict: Data model configuration dictionary
        """
        return self.config
    
    def __repr__(self):
        return f"DataModel(name='{self.name}', loglevel='{self.loglevel}')"

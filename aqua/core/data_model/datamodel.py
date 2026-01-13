"""
DataModel class for applying base coordinate transformations.
Provides a clean interface to CoordTransformer with caching.
"""
import xarray as xr
from aqua.core.logger import log_configure
from .coordtransformer import CoordTransformer
from .coord_utils import get_data_model


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
        name (str): Data model name (e.g., "aqua"). Default is "aqua". Other can be added.
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
        self.logger.debug("Initializing DataModel: %s", self.name)
        self.config = get_data_model(self.name)
    
    def apply(self, data: xr.Dataset, flip_coords=True) -> xr.Dataset:
        """
        Apply base data model transformations to dataset.
        
        Args:
            data (xr.Dataset): Input dataset
            flip_coords (bool): Whether to flip coordinate directions as per data model.
        
        Returns:
            xr.Dataset: Transformed dataset with standardized coordinates
        """
        self.logger.info("Applying data model: %s", self.name)
        return CoordTransformer(data, loglevel=self.loglevel).transform_coords(
            name=self.name, flip_coords=flip_coords
        )
    
    def get_config(self) -> dict:
        """
        Get the data model configuration.
        
        Returns:
            dict: Data model configuration dictionary
        """
        return self.config
    
    def __repr__(self):
        return f"DataModel(name='{self.name}', loglevel='{self.loglevel}')"

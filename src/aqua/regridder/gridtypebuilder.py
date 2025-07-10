"""This module base class for grid type builders and its extensions."""
import numpy as np
from aqua.logger import log_configure

class BaseGridTypeBuilder:
    """
    Base class for grid type builders.
    """
    requires_bounds = False
    bounds_error_message = "Data has no bounds, cannot create grid"
    logger_name = "BaseGridTypeBuilder"

    def __init__(
        self, data, masked, vert_coord,
        original_resolution, model_name, loglevel='warning'
        ):
        """
        Initialize the BaseGridTypeBuilder.

        Args: 
            data (xarray.Dataset): The dataset containing grid data.
            masked (str): The type of mask applied to the data.
            vert_coord (str): The vertical coordinate if applicable.
            loglevel (str): The logging level for the logger. Defaults to 'warning'.
        """
        self.data = data
        self.masked = masked
        self.vert_coord = vert_coord
        self.loglevel = loglevel
        self.original_resolution = original_resolution
        self.model_name = model_name
        self.logger = log_configure(log_level=loglevel, log_name=self.logger_name)

    def prepare(self):
        """
        Shared prepare logic for all grid type builders.
        Calls subclass get_metadata and handles bounds checking if required.
        """
        if self.requires_bounds and not self.has_bounds():
            raise ValueError(self.bounds_error_message)
        self.logger.info(f"Creating {self.logger_name} grid from data of size %s", self.data['mask'].size)
        metadata = self.get_metadata(self.data)
        basename = self.get_basename(metadata)
        self.logger.info("Basename %s", basename)
        self.logger.debug("Metadata: %s", metadata)
        return basename, metadata

    def get_basename(self, metadata):
        """
        Get the basename for the grid type.
        """
        
        if self.masked is None:
            basename = f"{metadata['aquagrid']}"
        elif self.masked == "land":
            raise NotImplementedError("Land masking is not implemented yet!")
        elif self.masked == "oce":
            basename = f"{self.model_name}_{self.original_resolution}_{metadata['aquagrid']}_oce"
            if self.vert_coord:
                basename += f"_{self.vert_coord}"
        return basename
    
    def has_bounds(self):
        """
        Check if the data has bounds.
        """
        if 'lon_bounds' in self.data.variables and 'lat_bounds' in self.data.variables:
            return True
        if 'lon_bnds' in self.data.variables and 'lat_bnds' in self.data.variables:
            return True
        return False

    def get_metadata(self, data):
        """
        Abstract method to get metadata for the grid type. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_metadata()")

class RegularGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build regular lon-lat grid files.
    """
    logger_name = "RegularGridTypeBuilder"
    requires_bounds = False

    def get_metadata(self, data):
        """
        Get metadata for the lon-lat grid based on the data size.
        Args: 
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the lon-lat grid, including nlon, nlat, cdogrid, and aquagrid.
        """
        nlon = data['lon'].size
        nlat = data['lat'].size
        cdogrid = f"r{nlon}x{nlat}"
        aquagrid = f"r{int(36000/nlon)}"
        if nlat % 2 == 1:
            aquagrid += "s"
        return {
            'nlon': nlon,
            'nlat': nlat,
            'cdogrid': cdogrid,
            'aquagrid': aquagrid
        }

class HealpixGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build HEALPix grid files.
    """
    logger_name = "HEALpixGridTypeBuilder"
    requires_bounds = True
    bounds_error_message = "Data has no bounds, cannot create Unstructured grid"

    def get_metadata(self, data):
        """
        Get metadata for the HEALPix grid based on the data size.
        Args: 
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the HEALPix grid, including nside, zoom, cdogrid, and aquagrid.
        """
        nside = np.sqrt(data['mask'].size / 12)
        zoom = int(np.log2(nside))
        return {
            'nside': nside,
            'zoom': zoom,
            'cdogrid': f"hp{int(nside)}_nested",
            'aquagrid': f"hpz{int(zoom)}_nested"
        }

class UnstructuredGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build Unstructured grid files.
    """
    logger_name = "UnstructuredGridTypeBuilder"
    requires_bounds = True
    bounds_error_message = "Data has no bounds, cannot create Unstructured grid"

    def get_metadata(self, data):
        """
        Get metadata for the Unstructured grid based on the data size.
        Args: 
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the Unstructured grid, including nlon, nlat, cdogrid, and aquagrid.
        """
        return {
            'aquagrid': self.model_name,
            'cdogrid': None,
            'size': data['mask'].size,
        }

class CurvilinearGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build Curvilinear grid files.
    """
    logger_name = "CurvilinearGridTypeBuilder"
    requires_bounds = True
    bounds_error_message = "Data has no bounds, cannot create Curvilinear grid"

    def get_metadata(self, data):
        """
        Get metadata for the Curvilinear grid based on the data size.
        Args: 
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the Curvilinear grid, including nlon, nlat, cdogrid, and aquagrid.
        """
        return {
            'aquagrid': self.model_name,
            'cdogrid': None,
            'size': data['mask'].size,
        }
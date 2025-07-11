"""This module base class for grid type builders and its extensions."""
from typing import Optional
import numpy as np
import xarray as xr
from cdo import Cdo
from aqua.logger import log_configure


class BaseGridTypeBuilder:
    """
    Base class for grid type builders.
    """
    requires_bounds = False
    bounds_error_message = "Data has no bounds, cannot create grid"
    logger_name = "BaseGridTypeBuilder"

    def __init__(
        self, vert_coord,
        original_resolution, model_name, loglevel='warning'
        ):
        """
        Initialize the BaseGridTypeBuilder.

        Args:
            vert_coord (str): The vertical coordinate if applicable.
            original_resolution (str): The original resolution of the data.
            model_name (str): The name of the model.
            loglevel (str): The logging level for the logger. Defaults to 'warning'.
        """
        self.masked = None
        self.vert_coord = vert_coord
        self.loglevel = loglevel
        self.original_resolution = original_resolution
        self.model_name = model_name
        self.logger = log_configure(log_level=loglevel, log_name=self.logger_name)
        self.cdo = Cdo()

    def prepare(self, data):
        """
        Shared prepare logic for all grid type builders.
        Calls subclass get_metadata and handles bounds checking if required.
        """
        if self.requires_bounds and not self.has_bounds(data):
            raise ValueError(self.bounds_error_message)
        #self.logger.info(f"Creating {self.logger_name} grid from data of size %s", self.data['mask'].size)
        metadata = self.get_metadata(data)
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
    
    def has_bounds(self, data):
        """
        Check if the data has bounds.
        """
        if 'lon_bounds' in data.variables and 'lat_bounds' in data.variables:
            return True
        if 'lon_bnds' in data.variables and 'lat_bnds' in data.variables:
            return True
        return False

    def get_metadata(self, data):
        """
        Abstract method to get metadata for the grid type. Must be implemented by subclasses.
        """
        raise NotImplementedError("Subclasses must implement get_metadata()")

    def data_reduction(self, data, gridtype, vert_coord=None):
        """
        Reduce the data to a single variable and time step.
        Args:
            data (xarray.Dataset): The dataset containing grid data.
            gridtype (GridInspector): The grid object containing GridType info.
            vert_coord (str, optional): The vertical coordinate if applicable.
        Returns:
            xarray.Dataset: The reduced data.
        """
        # extract first var from GridType, and guess time dimension from there
        var = next(iter(gridtype.variables))
        timedim = gridtype.time_dims[0] if gridtype.time_dims else None

        # temporal reduction
        if timedim:
            data = data.isel({timedim: 0}, drop=True)

        # load the variables and rename to mask for consistency
        load_vars = [var] + (gridtype.bounds or [])
        data = data[load_vars]
        data = data.rename({var: 'mask'})  

        # drop the remnant vertical coordinate if present
        if vert_coord and f"idx_{vert_coord}" in data.coords:
            data = data.drop_vars(f"idx_{vert_coord}")

        # set the mask variable to 1 where data is not null
        data['mask'] = xr.where(data['mask'].isnull(), np.nan, 1, keep_attrs=True)

        return data

    def select_2d_slice(self, data: xr.Dataset, vert_coord = None) -> xr.Dataset:
        """
        Select a 2D slice from the data along the vertical coordinate, if present.
        Args:
            data (xarray.Dataset): The dataset containing grid data.
            vert_coord (str, optional): The vertical coordinate if applicable.
        Returns:
            xarray.Dataset: The 2D-sliced data.
        """
        if vert_coord and vert_coord in data.dims:
            data2d = data.isel({vert_coord: 0})
        else:
            data2d = data
        if isinstance(data2d, xr.DataArray):
            data2d = data2d.to_dataset()
        return data2d

    def detect_mask_type(self, data: xr.Dataset) -> Optional[str]:
        """
        Detect the type of mask based on the data.
        Returns 'oce', 'land', or None.
        Args:
            data (xarray.Dataset): The dataset containing the 'mask' variable.
        Returns:
            Optional[str]: 'oce', 'land', or None if no mask is detected.
        """
        nan_count = float(data['mask'].isnull().sum().values) / data['mask'].size
        if nan_count == 0:
            return None
        if 0 < nan_count < 0.5:
            return "oce"
        if nan_count >= 0.5:
            return "land"
        raise ValueError(f"Unexpected nan count {nan_count}")

    def write_gridfile(self, input_file: str, output_file: str, metadata={}):
        """
        Write the grid file using CDO or by copying, depending on grid type.
        Can be overridden by subclasses for custom behavior.
        Args:
            input_file (str): Path to the temporary input file.
            output_file (str): Path to the final output file.
            metadata (dict, optional): Metadata dictionary from prepare().
            cdo: CDO instance for grid operations (optional).
        """
        if not metadata.get('cdogrid'):
            self.cdo.copy(input=input_file, output=output_file, options="-f nc4 -z zip")
        else:
            raise ValueError("cdogrid is not set in the metadata")
           

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

    def write_gridfile(self, input_file: str, output_file: str, metadata=None):
        """
        Write the grid file using CDO setgrid for regular grids.
        """
        if metadata is None:
            raise ValueError("metadata must be provided for RegularGridTypeBuilder.write_gridfile")
        self.cdo.setgrid(metadata['cdogrid'], input=input_file, output=output_file, options="-f nc4 -z zip")

class HealpixGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build HEALPix grid files.
    """
    logger_name = "HEALpixGridTypeBuilder"
    requires_bounds = False
    
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

    def write_gridfile(self, input_file: str, output_file: str, metadata=None, cdo=None, logger=None):
        """
        Write the grid file using CDO setgrid for HEALPix grids.
        """
        if metadata is None:
            raise ValueError("metadata and cdo must be provided for HealpixGridTypeBuilder.write_gridfile")
        self.cdo.setgrid(metadata['cdogrid'], input=input_file, output=output_file, options="-f nc4 -z zip")

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
"""This module base class for grid type builders and its extensions."""
from typing import Optional, Any, Dict
import numpy as np
import xarray as xr
from cdo import Cdo
from smmregrid import CdoGenerate, Regridder
from aqua.logger import log_configure


class BaseGridBuilder:
    """
    Base class for grid type builders.
    """
    requires_bounds = False
    bounds_error_message = "Data has no bounds, cannot create grid"
    logger_name = "BaseGridBuilder"

    def __init__(
        self, vert_coord,
        original_resolution, model_name, grid_name=None, loglevel='warning'
        ):
        """
        Initialize the BaseGridBuilder.

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
        self.grid_name = grid_name
        self.logger = log_configure(log_level=loglevel, log_name=self.logger_name)
        self.cdo = Cdo()

    def prepare(self, data):
        """
        Shared prepare logic for all grid type builders.
        Calls subclass get_metadata and handles bounds checking if required.

        Args:
            data (xarray.Dataset): The dataset containing grid data.

        Returns:
            tuple: The basename and metadata dictionary for the grid type.
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
        Get the basename for the grid type based on the metadata.
        
        Args:
            metadata (dict): The metadata for the grid type.

        Returns:
            str: The basename for the grid type.
        """
        # no oceanic masking: name is defined by the AQUA grid name
        # or alternative by the grid_name parameter provided to the GridBuilder
        if self.masked is None:
            if self.grid_name:
                self.logger.error(self.grid_name)
                basename = f"{self.grid_name}"
            else:
                basename = f"{metadata['aquagrid']}"

        # land masking: not supported yet
        elif self.masked == "land":
            raise NotImplementedError("Land masking is not implemented yet!")

        # oceanic masking: improvement needed. TODO: 
        # currently the name is defined by the AQUA the model name or 
        # by the grid_name parameter provided to the GridBuilder.
        # if both are provided, the grid_name parameter takes precedence.
        # the original resolution is added to the name if provided.
        # the aquagrid name is added to the name if it is different from the model name.
        # the vert_coord is added to the name if provided.
        # the oce suffix is added to the name.
        elif self.masked == "oce":
            if self.grid_name:
                basename = f"{self.grid_name}"
            else:
                basename = f"{self.model_name}"
            if self.original_resolution:
                basename += f"-{self.original_resolution}"
            if metadata['aquagrid'] != self.model_name:
                basename += f"_{metadata['aquagrid']}"
            basename += "_oce"
            if self.vert_coord:
                basename += f"_{self.vert_coord}"
            
        return basename

    def clean_attributes(self, data):
        """
        Clean the attributes of the data.

        """
        # cleaning attributes for variables
        for var in data.data_vars:
            data[var].attrs = {}

        # setting attributes for mask
        data['mask'].attrs['_FillValue'] = -9999
        data['mask'].attrs['missing_value'] = -9999
        data['mask'].attrs['long_name'] = 'mask'
        data['mask'].attrs['units'] = '1'
        data['mask'].attrs['standard_name'] = 'mask'

        # attribute checks for coordinates
        for coord in data.coords:

            # remove axis which can confuse CDO
            if not self.vert_coord or coord not in self.vert_coord:
                self.logger.debug("Removing axis for %s", coord)
                if 'axis' in data[coord].attrs:
                    del data[coord].attrs['axis']
   
            # remove bounds which can confuse CDO
            if not self.has_bounds(data):
                self.logger.debug("No bounds found for %s", coord)
                if 'bounds' in data[coord].attrs:
                    self.logger.debug("Removing bounds for %s", coord)
                    del data[coord].attrs['bounds']

        # adding vertical properties
        if self.vert_coord:
            data[self.vert_coord].attrs['axis'] = 'Z'
        
        return data
    
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
        # extract first var from GridType and get the attributes of the original variable
        var = next(iter(gridtype.variables))
        attrs = data[var].attrs.copy()

        # guess time dimension from the GridType
        timedim = gridtype.time_dims[0] if gridtype.time_dims else None

        # temporal reduction
        if timedim:
            data = data.isel({timedim: 0}, drop=True)

        # load the variables and rename to mask for consistency
        space_bounds = [bound for bound in gridtype.bounds if not 'time' in bound]
        load_vars = [var] + space_bounds #(gridtype.bounds or [])
        data = data[load_vars]
        data = data.rename({var: 'mask'})

        # drop the remnant vertical coordinate if present
        if vert_coord and f"idx_{vert_coord}" in data.coords:
            data = data.drop_vars(f"idx_{vert_coord}")

        # set the mask variable to 1 where data is not null
        data['mask'] = xr.where(data['mask'].isnull(), np.nan, 1)

        # preserve the attributes of the original variable
        data['mask'].attrs = attrs

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
        self.logger.info("Nan count: %s", nan_count)
        if nan_count == 0:
            self.masked = None
        elif 0 < nan_count < 0.5:
            self.masked = "oce"
        elif nan_count >= 0.5:
            self.masked = "land"
        else:
            raise ValueError(f"Unexpected nan count {nan_count}")

    def verify_weights(
        self, filename, target_grid="r180x90", metadata=None
    ):
        """
        Verify the creation of the weights from the grid file.
        """
        if metadata is None:
            raise ValueError("metadata must be provided for BaseGridTypeBuilder.verify_weights")
        remap_method = metadata.get('remap_method', "con")
        cdo_options = metadata.get('cdo_options', "")
        try:
            self.logger.info("Generating weights for %s with method %s and vert_coord %s", filename, remap_method, self.vert_coord)
            generator = CdoGenerate(source_grid=filename, target_grid=target_grid, cdo_options=cdo_options, loglevel=self.loglevel)
            weights = generator.weights(method=remap_method, vert_coord=self.vert_coord)
            self.logger.info("Weights %s generated successfully for %s!!! This grid file is approved for AQUA, take a bow!", remap_method, filename)
        except Exception as e:
            self.logger.error("Error generating weights, something is wrong with weights generation: %s", e)
            raise
        try:
            regridder = Regridder(weights=weights, cdo_options=cdo_options, loglevel=self.loglevel)
            data = xr.open_dataset(filename)
            regridder.regrid(data)
            self.logger.info("Grid %s regridded successfully for %s!!! This grid file is approved for AQUA, fly me to the moon!", remap_method, filename)
        except Exception as e:
            self.logger.error("Error regridding, something is wrong with the regridding: %s", e)
            raise

    def write_gridfile(self, input_file: str, output_file: str, metadata=None):
        """
        Write the grid file using CDO or by copying, depending on grid type.
        Can be overridden by subclasses for custom behavior.
        Args:
            input_file (str): Path to the temporary input file.
            output_file (str): Path to the final output file.
            metadata (dict, optional): Metadata dictionary from prepare().
            cdo: CDO instance for grid operations (optional).
        """
        if not metadata or not metadata.get('cdogrid'):
            self.cdo.copy(input=input_file, output=output_file, options="-f nc4 -z zip")
        else:
            raise ValueError("cdogrid is not set in the metadata")

    @staticmethod
    def create_grid_entry_block(
        gridtype: Any,
        basepath: str,
        vert_coord: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """ Create a grid entry for the gridtype.

        Args:
            gridtype (GridType): The smmregrid GridType object containing grid information.
            basepath (str): The base path for the grid file.
            vert_coord (str, optional): The vertical coordinate if applicable.
            metadata (dict, optional): Metadata dictionary from prepare().

        Returns:
            dict: The grid entry block.
        """
        
        grid_block = {
            'path': f"{basepath}.nc",
            'space_coord': gridtype.horizontal_dims,
        }
        if vert_coord:
            grid_block['vert_coord'] = vert_coord
            grid_block['path'] = {vert_coord: f"{basepath}.nc"}
        # add metadata if provided
        if metadata:
            if 'cdo_options' in metadata:
                grid_block['cdo_options'] = metadata['cdo_options']
            if 'remap_method' in metadata:
                if metadata['remap_method'] != 'con':
                    grid_block['remap_method'] = metadata['remap_method']
        return grid_block

    @staticmethod
    def create_grid_entry_name(
        name: str,
        vert_coord: Optional[str]
    ) -> str:
        """Create a grid entry name based on the grid type and vertical coordinate.
        
        Args:
            name (str): The base name for the grid file.
            vert_coord (str, optional): The vertical coordinate if applicable.

        Returns:
            str: The grid entry name.
        """

        if vert_coord is not None:
            name = name.replace(vert_coord, '3d')  
        # Replace _hpz10 with -hpz7
        name = name.replace('_oce_', '_').replace('_', '-')

        return name
           

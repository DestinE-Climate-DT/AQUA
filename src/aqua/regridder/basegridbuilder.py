"""This module base class for grid type builders and its extensions."""
from typing import Optional, Any, Dict
import numpy as np
import xarray as xr
from cdo import Cdo
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
        original_resolution, model_name, loglevel='warning'
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
            # TODO: is there a better way to verify that we can generate the weights?
            self.logger.info("Verifying the creation of the weights from the grid file")
            getattr(self.cdo, f"gen{remap_method}")(target_grid, input=filename, options=f"{cdo_options} -f nc")
            self.logger.info("Weights %s generated successfully for %s!!! This grid file is approved for AQUA, take a bow!", remap_method, filename)
        except Exception as e:
            self.logger.error("Error generating weights, something is wrong with the obtained file: %s", e)
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
            'cdo_options': '--force',
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
            name = name.replace(vert_coord, '3d')  # Replace _hpz10 with -hpz7
    
        return name.replace('_oce_', '_').replace('_', '-')
           

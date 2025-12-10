"""This module base class for grid type builders and its extensions."""
import os
from typing import Optional, Dict
import numpy as np
import xarray as xr
from cdo import Cdo
from smmregrid import CdoGenerate, Regridder, GridType
from aqua.core.logger import log_configure


class BaseGridBuilder:
    """
    Base class for grid type builders.
    """
    requires_bounds = False
    bounds_error_message = "Data has no bounds, cannot create grid"
    logger_name = "BaseGridBuilder"
    CDOZIP = "-f nc4 -z zip"

    def __init__(
        self,
        vert_coord: str,
        original_resolution: str,
        model_name: str,
        grid_name: Optional[str] = None,
        loglevel: str = 'warning'
    ) -> None:
        """
        Initialize the BaseGridBuilder.

        Args:
            vert_coord (str): The vertical coordinate if applicable.
            original_resolution (str): The original resolution of the data.
            model_name (str): The name of the model.
            grid_name (Optional[str]): The name of the grid, if applicable.
            loglevel (str, optional): The logging level for the logger. Defaults to 'warning'.
        """
        self.masked = None
        self.vert_coord = vert_coord
        self.loglevel = loglevel
        self.original_resolution = original_resolution
        self.model_name = model_name
        self.grid_name = grid_name
        self.logger = log_configure(log_level=loglevel, log_name=self.logger_name)
        self.cdo = Cdo()

    def clean_attributes(self, data: xr.Dataset) -> xr.Dataset:
        """
        Clean the attributes of the data.

        Args:
            data (xarray.Dataset): The dataset to clean attributes for.
        Returns:
            xarray.Dataset: The dataset with cleaned attributes.
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
            if not self.vert_coord or coord != self.vert_coord:
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

    def has_bounds(self, data: xr.Dataset) -> bool:
        """
        Check if the data has bounds.

        Args:
            data (xarray.Dataset): The dataset to check for bounds.
        Returns:
            bool: True if bounds are present, False otherwise.
        """
        if 'lon_bounds' in data.variables and 'lat_bounds' in data.variables:
            return True
        if 'lon_bnds' in data.variables and 'lat_bnds' in data.variables:
            return True
        return False

    def get_metadata(self, data: xr.Dataset) -> dict:
        """
        Abstract method to get metadata for the grid type. Must be implemented by subclasses.

        Args:
            data (xarray.Dataset): The dataset to extract metadata from.
        Returns:
            dict: Metadata dictionary for the grid type.
        """
        raise NotImplementedError("Subclasses must implement get_metadata()")

    def data_reduction(self, data: xr.Dataset, gridtype: GridType, vert_coord: Optional[str] = None) -> xr.Dataset:
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
        load_vars = [var] + space_bounds  # (gridtype.bounds or [])
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

    def select_2d_slice(self, data: xr.Dataset, vert_coord: Optional[str] = None) -> xr.Dataset:
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
            self.masked = "ocean"
        elif nan_count >= 0.5:
            self.masked = "land"
        else:
            raise ValueError(f"Unexpected nan count {nan_count}")
        return self.masked

    def verify_weights(
        self, filename: str, metadata: Dict, target_grid: str = "r180x90"
    ) -> None:
        """
        Verify the creation of the weights from the grid file.

        Args:
            filename (str): Path to the grid file. Could be also a CDO grid name.
            metadata (dict): Metadata dictionary for weights generation.
            target_grid (str, optional): Target grid for weights generation. Defaults to "r180x90".
        Returns:
            None
        """
        remap_method = metadata.get('remap_method', "con")
        cdo_options = metadata.get('cdo_options', "")
        try:
            self.logger.info(
                "Generating weights for %s with method %s and vert_coord %s",
                filename,
                remap_method,
                self.vert_coord)
            generator = CdoGenerate(
                source_grid=filename,
                target_grid=target_grid,
                cdo_options=cdo_options,
                loglevel=self.loglevel)
            weights = generator.weights(method=remap_method, vert_coord=self.vert_coord)
            self.logger.info(
                "Weights %s generated successfully for %s!!! This grid file is approved for AQUA, take a bow!",
                remap_method,
                filename)
        except Exception as e:
            self.logger.error("Error generating weights, something is wrong with weights generation: %s", e)
            raise
        try:
            regridder = Regridder(weights=weights, cdo_options=cdo_options, loglevel=self.loglevel)
            if os.path.exists(filename):
                data = xr.open_dataset(filename)
            else:
                data = self.cdo.const(f'1,{filename}', options=cdo_options, returnXDataset=True)
            regridder.regrid(data)
            self.logger.info(
                "Grid %s regridded successfully for %s!!! This grid file is approved for AQUA, fly me to the moon!",
                remap_method,
                filename)
        except Exception as e:
            self.logger.error("Error regridding, something is wrong with the regridding: %s", e)
            raise

    def write_gridfile(self, input_file: str, output_file: str, metadata: dict) -> None:
        """
        Write the grid file using CDO or by copying, depending on grid type.
        Can be overridden by subclasses for custom behavior.
        Args:
            input_file (str): Path to the temporary input file.
            metadata (dict): Metadata dictionary from prepare().
            output_file (str): Path to the final output file.
        Returns:
            None
        """
        if metadata.get('cdogrid'):
            self.logger.info("Writing grid file to %s with CDO grid %s", output_file, metadata['cdogrid'])
            self.cdo.setgrid(metadata['cdogrid'], input=input_file, output=output_file, options=self.CDOZIP)
        else:
            self.logger.info("Writing grid file to %s", output_file)
            self.cdo.copy(input=input_file, output=output_file, options=self.CDOZIP)

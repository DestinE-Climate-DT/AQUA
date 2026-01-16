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
        # Remove scalar coordinates that aren't dimensions (e.g., height)
        scalar_coords_to_drop = []
        for coord in data.coords:
            if coord not in data.dims and data[coord].ndim == 0:
                self.logger.debug(f"Removing scalar coordinate: {coord}")
                scalar_coords_to_drop.append(coord)
        if scalar_coords_to_drop:
            data = data.drop_vars(scalar_coords_to_drop)

        # Clean all data variable attributes first
        for var in data.data_vars:
            data[var].attrs = {}

        # Set attributes for mask
        data['mask'].attrs['_FillValue'] = -9999
        data['mask'].attrs['missing_value'] = -9999
        data['mask'].attrs['long_name'] = 'mask'
        data['mask'].attrs['units'] = '1'
        data['mask'].attrs['standard_name'] = 'mask'

        # Set attributes for bounds variables (they are data_vars, not coords)
        for bounds_var in ['lon_bnds', 'lat_bnds', 'lon_bounds', 'lat_bounds']:
            if bounds_var in data.data_vars:
                # Keep bounds variables clean - no extra attributes
                data[bounds_var].attrs = {}

        # Clean coordinate attributes
        for coord in data.coords:
            # Clear all attributes first
            data[coord].attrs = {}
            
            # Add appropriate axis attributes for spatial/vertical coordinates
            if coord == 'lon':
                data[coord].attrs['axis'] = 'X'
                data[coord].attrs['standard_name'] = 'longitude'
                data[coord].attrs['units'] = 'degrees_east'
                # Link to bounds if they exist
                if 'lon_bnds' in data.variables:
                    data[coord].attrs['bounds'] = 'lon_bnds'
                elif 'lon_bounds' in data.variables:
                    data[coord].attrs['bounds'] = 'lon_bounds'
            elif coord == 'lat':
                data[coord].attrs['axis'] = 'Y'
                data[coord].attrs['standard_name'] = 'latitude'
                data[coord].attrs['units'] = 'degrees_north'
                # Link to bounds if they exist
                if 'lat_bnds' in data.variables:
                    data[coord].attrs['bounds'] = 'lat_bnds'
                elif 'lat_bounds' in data.variables:
                    data[coord].attrs['bounds'] = 'lat_bounds'
            elif self.vert_coord and coord == self.vert_coord:
                data[coord].attrs['axis'] = 'Z'
            elif coord in ['bnds', 'bounds']:
                # No axis for bounds dimension
                pass

        # Fix bounds dimensions to prevent lon_2, lat_2 issues
        data = self._fix_bounds_dims(data)
        
        # # Add cell area for unstructured/curvilinear grids (needed for CDO fldmean)
        # data = self._add_cell_area(data)

        return data
    
    def _add_cell_area(self, data: xr.Dataset) -> xr.Dataset:
        """
        Add cell area variable for unstructured/curvilinear grids.
        
        CDO needs cell area information for area-weighted operations like fldmean.
        This calculates the area from the bounds if available.
        
        Args:
            data (xr.Dataset): Dataset to add cell area to.
        Returns:
            xr.Dataset: Dataset with cell area added.
        """
        # Only add cell area if we have bounds
        if not self.has_bounds(data):
            return data
        
        # Check if this is a 2D grid that needs cell area
        if 'lat' in data.dims and 'lon' in data.dims:
            try:
                import numpy as np
                
                # Get bounds - check which variables exist
                lat_bnds = None
                lon_bnds = None
                
                if 'lat_bnds' in data.variables:
                    lat_bnds = data['lat_bnds']
                elif 'lat_bounds' in data.variables:
                    lat_bnds = data['lat_bounds']
                    
                if 'lon_bnds' in data.variables:
                    lon_bnds = data['lon_bnds']
                elif 'lon_bounds' in data.variables:
                    lon_bnds = data['lon_bounds']
                
                if lat_bnds is None or lon_bnds is None:
                    self.logger.debug("Missing bounds variables for cell area calculation")
                    return data
                    
                # Calculate cell area using spherical geometry
                # Area = R^2 * |lon2-lon1| * |sin(lat2)-sin(lat1)|
                R_earth = 6371000.0  # Earth radius in meters
                
                lat_bnds_rad = np.deg2rad(lat_bnds.values)
                lon_bnds_rad = np.deg2rad(lon_bnds.values)
                
                # Calculate differences
                dlat = np.abs(np.sin(lat_bnds_rad[:, 1]) - np.sin(lat_bnds_rad[:, 0]))
                dlon = np.abs(lon_bnds_rad[:, 1] - lon_bnds_rad[:, 0])
                
                # Broadcast to 2D
                dlat_2d = dlat[:, np.newaxis]
                dlon_2d = dlon[np.newaxis, :]
                
                # Calculate area
                cell_area = R_earth**2 * dlon_2d * dlat_2d
                
                # Add to dataset
                data['cell_area'] = xr.DataArray(
                    cell_area,
                    dims=['lat', 'lon'],
                    coords={'lat': data['lat'], 'lon': data['lon']},
                    attrs={
                        'standard_name': 'cell_area',
                        'long_name': 'Grid cell area',
                        'units': 'm2'
                    }
                )
                self.logger.info("Added cell_area variable for CDO operations")
                    
            except Exception as e:
                self.logger.warning(f"Could not calculate cell area: {e}")
        
        return data

    def _fix_bounds_dims(self, data: xr.Dataset) -> xr.Dataset:
        """
        Fix bounds dimensions to ensure they match coordinate dimensions.
        
        When bounds have mismatched dimensions, xarray creates new dimensions 
        (lon_2, lat_2) during to_netcdf(). This method rebuilds bounds with 
        correct dimensions.
        
        Args:
            data (xr.Dataset): Dataset with potential dimension mismatches.
        Returns:
            xr.Dataset: Dataset with fixed bounds dimensions.
        """
        bounds_map = {
            'lon_bnds': 'lon',
            'lat_bnds': 'lat', 
            'lon_bounds': 'lon',
            'lat_bounds': 'lat'
        }
        
        for bounds_var, coord_name in bounds_map.items():
            if bounds_var in data.variables and coord_name in data.dims:
                bounds_dims = list(data[bounds_var].dims)
                self.logger.debug(f"Checking {bounds_var} with dims: {bounds_dims}")
                
                # Check if the first dimension doesn't match the coordinate
                if bounds_dims[0] != coord_name:
                    self.logger.warning(
                        f"{bounds_var} has wrong dimension {bounds_dims[0]}, "
                        f"should be {coord_name}. Rebuilding bounds."
                    )
                    
                    # Get the bounds data and rebuild with correct dimensions
                    bounds_data = data[bounds_var].values
                    bnds_dim = bounds_dims[-1]  # Usually 'bnds' or 'bounds'
                    
                    # Create new bounds DataArray with correct dimensions
                    new_bounds = xr.DataArray(
                        bounds_data,
                        dims=[coord_name, bnds_dim],
                        coords={coord_name: data[coord_name], bnds_dim: data[bnds_dim]},
                        attrs=data[bounds_var].attrs
                    )
                    
                    # Replace in dataset
                    data = data.drop_vars(bounds_var)
                    data[bounds_var] = new_bounds
                    
                    self.logger.info(f"Fixed {bounds_var} dimensions to [{coord_name}, {bnds_dim}]")
        
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
            self.logger.info("Writing grid file to %s without CDO processing", output_file)
            # Read the input file with xarray
            data = xr.open_dataset(input_file)
            
            # Build encoding to prevent _FillValue on coordinates and properly encode variables
            encoding = {}
            
            # Encode data variables with compression
            for var in data.data_vars:
                encoding[var] = {'zlib': True, 'complevel': 1}
                # Ensure proper FillValue for mask
                if var == 'mask':
                    encoding[var]['_FillValue'] = -9999.0
                else:
                    # For bounds and cell_area, no FillValue
                    encoding[var]['_FillValue'] = None
            
            # Encode coordinates without _FillValue
            for coord in data.coords:
                encoding[coord] = {'_FillValue': None}
            
            # Write with explicit encoding
            data.to_netcdf(output_file, encoding=encoding, format='NETCDF4')
            data.close()

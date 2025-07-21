"""
This module contains the specific grid type builders for the different grid types.
"""

import numpy as np
from .basegridbuilder import BaseGridBuilder

class RegularGridBuilder(BaseGridBuilder):
    """
    Class to build regular lon-lat grid files.
    """
    logger_name = "RegularGridBuilder"
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
            'aquagrid': aquagrid,
            'remap_method': "con",
            'kind': 'regular'
        }

    def write_gridfile(self, input_file: str, output_file: str, metadata=None):
        """
        Write the grid file using CDO setgrid for regular grids.
        """
        if metadata is None:
            raise ValueError("metadata must be provided for RegularGridTypeBuilder.write_gridfile")
        self.cdo.setgrid(metadata['cdogrid'], input=input_file, output=output_file, options=self.CDOZIP)

class HealpixGridBuilder(BaseGridBuilder):
    """
    Class to build HEALPix grid files.
    """
    logger_name = "HEALpixGridBuilder"
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
            'aquagrid': f"hpz{int(zoom)}_nested",
            'remap_method': "con",
            'cdo_options': '--force',
            'kind': 'healpix'
        }

    def write_gridfile(self, input_file: str, output_file: str, metadata=None):
        """
        Write the grid file using CDO setgrid for HEALPix grids.
        """
        if metadata is None:
            raise ValueError("metadata and cdo must be provided for HealpixGridTypeBuilder.write_gridfile")
        self.cdo.setgrid(metadata['cdogrid'], input=input_file, output=output_file, options=self.CDOZIP)

class UnstructuredGridBuilder(BaseGridBuilder):
    """
    Class to build Unstructured grid files.
    """
    logger_name = "UnstructuredGridBuilder"
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
        if not self.grid_name:
            raise ValueError("Grid name is not set for UnstructuredGrid, please provide it")

        return {
            'aquagrid': self.grid_name,
            'cdogrid': None,
            'size': data['mask'].size,
            'remap_method': "con",
            'kind': 'unstructured',
        }

class CurvilinearGridBuilder(BaseGridBuilder):
    """
    Class to build Curvilinear grid files.
    """
    logger_name = "CurvilinearGridBuilder"
    requires_bounds = False

    def get_metadata(self, data):
        """
        Get metadata for the Curvilinear grid based on the data size.
        Args: 
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the Curvilinear grid, including nlon, nlat, cdogrid, and aquagrid.
        """
        if not self.grid_name:
            raise ValueError("Grid name is not set for CurvilinearGrid, please provide it")

        if self.has_bounds(data):
            remap_method = "con"
        else:
            self.logger.warning("Bounds not found, using bilinear remapping for Curvilinear grid")
            remap_method = "bil"

        return {
            'aquagrid': self.grid_name,
            'cdogrid': None,
            'size': data['mask'].size,
            'remap_method': remap_method,
            'kind': 'curvilinear'
        }

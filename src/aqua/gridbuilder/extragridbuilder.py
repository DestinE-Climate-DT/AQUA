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
        aquagrid = f"r{int(36000 / nlon)}"
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


class GaussianRegularGridBuilder(BaseGridBuilder):
    """
    Class to build Gaussian regular lon-lat grid files.
    """
    logger_name = "GaussianRegularGridBuilder"
    requires_bounds = False

    def get_metadata(self, data):
        """
        Get metadata for the Gaussian regular lon-lat grid based on the data size.
        Args:
            data (xarray.Dataset): The dataset containing grid data.
        Returns:
            dict: Metadata for the Gaussian regular lon-lat grid, including nlon, nlat, cdogrid, and aquagrid.
        """

        nlon = data['lon'].size
        nlat = data['lat'].size
        if data['lat'][0] > data['lat'][-1]:
            cdogrid = f"F{int(nlat / 2)}"
            aquagrid = f"F{int(nlat / 2)}"
        else:
            raise ValueError("Latitude values are not from North to South, cannot build Gaussian regular grid")
        return {
            'nlon': nlon,
            'nlat': nlat,
            'cdogrid': cdogrid,
            'aquagrid': aquagrid,
            'spectral_truncation': nlat - 1,
            'remap_method': "con",
            'kind': 'gaussianregular'
        }


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

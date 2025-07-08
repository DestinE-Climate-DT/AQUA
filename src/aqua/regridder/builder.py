"""Module for aqua grid build"""
import os
import numpy as np
from cdo import Cdo
from smmregrid import GridInspector
from aqua import Reader
from aqua.regridder.regridder_util import detect_grid
from aqua.logger import log_configure

class GridBuilder():
    """Class to build grids from data sources."""

    def __init__(self, model, exp, source, original_resolution=None, loglevel='warning'):
        """
        Initialize the GridBuilder with a reader instance.

        Args:
            reader (Reader): An instance of the Reader class.
        """
        self.model = model
        self.exp = exp
        self.source = source
        self.logger = log_configure(log_level=loglevel, log_name='GridBuilder')
        self.loglevel = loglevel
        self.reasonable_vert_coords = ['depth_full', 'depth_half', 'level']
        self.original_resolution = original_resolution
        self.cdo = Cdo()

    def retrieve(self):
        """
        Retrieve the grid data based on the model, experiment, and source.

        Returns:
            xarray.Dataset: The retrieved grid data.
        """
        reader = Reader(model=self.model, exp=self.exp, source=self.source, loglevel=self.loglevel,
                        areas=False, fix=False)
        return reader.retrieve()
    
    def detect_grid(self, data):
        """
        Detect the grid type based on the structure of the data.

        Args:
            data (xarray.Dataset): The dataset containing grid data.

        Returns:
            str: The detected grid type.
        """
        
        return detect_grid(data)
    
    def build(self):
        """
        Retrieve and build the grid data for all gridtypes available.
        """
        data = self.retrieve()
        gridtypes = GridInspector(data).get_gridtype()
        for gridtype in gridtypes:
            self._build_gridtype(data, gridtype)
            
    def _build_gridtype(self, data, gridtype):
        """
        Build the grid data based on the detected grid type.

        Args:
            data (xarray.Dataset): The dataset containing grid data.
            gridtype (str): The detected grid type.

        """
        self.logger.info("Detected grid type: %s", gridtype)
        kind = self.detect_grid(data)
        self.logger.info("Grid type is: %s", kind)

        # data reduction
        var = next(iter(gridtype.variables))
        timedim = gridtype.time_dims[0] if gridtype.time_dims else None
        data = data[var]
        if timedim:
            data = data.isel({timedim: 0})

        vert_coord = list(set(self.reasonable_vert_coords) & set(data.coords))
        vert_coord = vert_coord[0] if vert_coord else None
        if vert_coord:
            self.logger.debug("Modifying level axis attributes as Z")
            data[vert_coord].attrs['axis'] = 'Z'

        # store the data in a netcdf file
        filename_tmp = f"{self.model}_{self.exp}_{self.source}_{var}.nc"
        self.logger.info("Saving tmp data in %s", filename_tmp)
        data.to_netcdf(filename_tmp)

        if kind == 'Healpix':
            filename, metadata = self.prepare_healpix(data, vert_coord)
        else:
            raise NotImplementedError(f"Grid type {kind} is not implemented yet")
        
        if os.path.exists(filename):
            self.logger.warning('File %s already exists, removing it', filename)
            os.remove(filename)
            self.logger.info('Calling CDO to set the grid %s to %s', metadata['cdogrid'], filename)
        self.cdo.setgrid(metadata['cdogrid'], input=filename_tmp, output=filename, options="-f nc4 -z zip")
        self.logger.info('Removing temporary file %s', filename_tmp)
        os.remove(filename_tmp)

    
    def prepare_healpix(self, data, vert_coord=None):
        """
        Create a HEALPix grid from the data.

        Args:
            data (xarray.Dataset): The dataset containing grid data.

        Returns:
            xarray.Dataset: The HEALPix grid data.
        """
        # Implement the logic to create a HEALPix grid
        # This is a placeholder implementation
        self.logger.info("Creating HEALPix grid from data %s", data.size)
        metadata = self._get_healpix_metadata(data)

        # Construct the filename for the output grid
        filename = self.model
        if vert_coord and self.original_resolution:
            filename += f"-{self.original_resolution}"
        filename += f"-{metadata['aquagrid']}"
        if vert_coord:
            filename += f"_oce-{vert_coord}"
        filename += ".nc"
        self.logger.info("Saving HEALPix grid to %s", filename)
        return filename, metadata
    
    def _get_healpix_metadata(self, data):

        # Implement the logic to extract HEALPix data
        # This is a placeholder implementation
        nside = np.sqrt(data.size / 12)
        zoom = int(np.log2(nside))
        return {
            'nside': nside,
            'zoom': zoom,
            'cdogrid': f"hp{int(nside)}_nested",
            'aquagrid': f"hpz{int(zoom)}_nested"
        }

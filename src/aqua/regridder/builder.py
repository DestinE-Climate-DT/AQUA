"""Module for aqua grid build"""
import os
import re
from glob import glob
import numpy as np
import xarray as xr
from cdo import Cdo
from smmregrid import GridInspector
from aqua import Reader
from aqua.regridder.regridder_util import detect_grid
from aqua.logger import log_configure, log_history

class GridBuilder():
    """Class to build grids from data sources."""

    def __init__(
            self, model, exp, source,
            outdir='.', original_resolution=None, loglevel='warning'
        ):
        """
        Initialize the GridBuilder with a reader instance.

        Args:
            reader (Reader): An instance of the Reader class.
        """
        self.model = model
        self.exp = exp
        self.source = source
        self.outdir = outdir
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
    
    def build(self, rebuild=False, version=None):
        """
        Retrieve and build the grid data for all gridtypes available.
        """
        data = self.retrieve()
        gridtypes = GridInspector(data).get_gridtype()
        for gridtype in gridtypes:
            self._build_gridtype(data, gridtype, rebuild=rebuild, version=version)
            
    def _build_gridtype(self, data, gridtype, rebuild=False, version=None):
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
        data = self._data_reduction(data, gridtype)

        # vertical coordinate detection
        vert_coord = list(set(self.reasonable_vert_coords) & set(data.coords))
        vert_coord = vert_coord[0] if vert_coord else None
        if vert_coord:
            self.logger.debug("Modifying level axis attributes as Z")
            data[vert_coord].attrs['axis'] = 'Z'

        # detect the mask type
        masked = self._detect_mask_type(data, vert_coord=vert_coord)

        # uniform the data
        mask = xr.where(data.notnull(), 1, data)
        mask.name = "mask"
        mask = mask.to_dataset(name=mask.name)

        # add history attribute
        log_history(mask, msg=f'Gridfile generated with GridBuilder from {self.model}_{self.exp}_{self.source}')

        # store the data in a netcdf file
        filename_tmp = f"{self.model}_{self.exp}_{self.source}.nc"
        self.logger.info("Saving tmp data in %s", filename_tmp)
        mask.to_netcdf(filename_tmp)

        if kind == 'Healpix':
            basename, metadata = self.prepare_healpix(data, masked, vert_coord)
        else:
            raise NotImplementedError(f"Grid type {kind} is not implemented yet")
        
        basepath = os.path.join(self.outdir, basename)
        # check if version has been defined and set the filename accordingly
        if version is not None:
            if not isinstance(version, int):
                raise ValueError(f"Version must be an integer, got {version}")
            filename = f"{basepath}_v{version}.nc"
        else:
            filename = f"{basepath}.nc"

        # verify the existence of files and handle versioning
        existing_files = glob(f"{basepath}*.nc")
        self.logger.info("Existing files: %s", existing_files)
        if existing_files and version is None:
            pattern = re.compile(r"_v\d+\.nc$")
            check_version = [bool(pattern.search(file)) for file in existing_files]
            if any(check_version):
                raise ValueError(f"Versioned files already exist for {basepath}. Please specify a version")

        if os.path.exists(filename):
            if rebuild:
                self.logger.warning('File %s already exists, removing it', filename)
                os.remove(filename)
            else:
                self.logger.error("File %s already exists, skipping", filename)
                return
        
        self.logger.info('Calling CDO to set the grid %s to %s', metadata['cdogrid'], filename)
        self.cdo.setgrid(metadata['cdogrid'], input=filename_tmp, output=filename, options="-f nc4 -z zip")
        self.logger.info('Removing temporary file %s', filename_tmp)
        os.remove(filename_tmp)

    @staticmethod
    def _data_reduction(data, gridtype):
        """
        Reduce the data to a single variable and time step.

        Args:
            data (xarray.Dataset): The dataset containing grid data.
            var (str, optional): The variable to select. Defaults to None.
            timedim (str, optional): The time dimension to select. Defaults to None.

        Returns:
            xarray.DataArray: The reduced data.
        """
        # data reduction
        var = next(iter(gridtype.variables))
        timedim = gridtype.time_dims[0] if gridtype.time_dims else None
        data = data[var]
        if timedim:
            data = data.isel({timedim: 0})
        return data
    
    @staticmethod
    def _detect_mask_type(data, vert_coord=None):
        """ Detect the type of mask based on the data and grid kind. """
        if vert_coord and vert_coord in data.coords:
            maskdata = data.isel({vert_coord: 0})
        else:
            maskdata = data
        nan_count = maskdata.isnull().sum().values/maskdata.size
        if nan_count == 0:
            return None
        if 0.2 < nan_count < 0.5:
            return "land"
        if nan_count >= 0.5:
            return "oce"
        
        raise ValueError(f"Unexpected nan count {nan_count}")
    

    def prepare_healpix(self, data, masked=None, vert_coord=None):
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

        if masked is None:
            basename = f"{metadata['aquagrid']}_atm"
        elif masked == "land":
            raise NotImplementedError("Land masking is not implemented yet!")
        elif masked == "oce":
            basename = f"{self.model}_{self.original_resolution}_{metadata['aquagrid']}_oce"
            if vert_coord:
                basename += f"_{vert_coord}"

        # Construct the filename for the output grid
        self.logger.info("Basename %s", basename)
        self.logger.debug("Metadata: %s", metadata)
        return basename, metadata
    
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

"""This module base class for grid type builders and its extensions."""
import numpy as np
from aqua.logger import log_configure

class BaseGridTypeBuilder:
    """
    Base class for grid type builders.
    """

    def __init__(self, data, masked, vert_coord, loglevel='warning'):
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

    def prepare(self):
        """Return (basename, metadata) for this grid type."""
        raise NotImplementedError

class HealpixGridTypeBuilder(BaseGridTypeBuilder):
    """
    Class to build HEALPix grid files.
    """
    def __init__(
        self, data, masked, vert_coord, 
        model_name=None, original_resolution=None, loglevel='warning'
    ):
        super().__init__(data, masked, vert_coord)
        self.original_resolution = original_resolution
        self.model_name = model_name
        self.logger = log_configure(log_level=loglevel, log_name='GridTypeBuilder')
        
    def prepare(self):
        
        self.logger.info("Creating HEALPix grid from data of size %s", self.data['mask'].size)
        metadata = self.get_healpix_metadata(self.data)

        if self.masked is None:
            basename = f"{metadata['aquagrid']}_atm"
        elif self.masked == "land":
            raise NotImplementedError("Land masking is not implemented yet!")
        elif self.masked == "oce":
            basename = f"{self.model_name}_{self.original_resolution}_{metadata['aquagrid']}_oce"
            if self.vert_coord:
                basename += f"_{self.vert_coord}"

        self.logger.info("Basename %s", basename)
        self.logger.debug("Metadata: %s", metadata)
        return basename, metadata

    @staticmethod
    def get_healpix_metadata(data):
        """"
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

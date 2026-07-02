"""fdb_xarray — expose Destination Earth Climate-DT FDB data as xarray.Datasets."""

from .core import open_climate_dt, to_dataset

__all__ =  ["open_climate_dt", "to_dataset"]
__version__ = "0.1.0"

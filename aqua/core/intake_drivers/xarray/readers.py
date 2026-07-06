from intake import BaseReader
from intake.readers.datatypes import NetCDF3, Zarr

from .openers import open_netcdf, open_zarr


class NetCDFDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {"xarray"}
    optional_imports = {}
    func = "open_netcdf"
    implements = {
        NetCDF3,
    }

    def _read(self, data, **kwargs):
        params = data.to_dict()
        params.update(kwargs)
        return open_netcdf(**params)


class ZarrDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {"xarray", "zarr"}
    optional_imports = {}
    func = "open_zarr"
    implements = {
        Zarr,
    }

    def _read(self, data, **kwargs):
        params = data.to_dict()
        params.update(kwargs)
        return open_zarr(**params)

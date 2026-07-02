"""Backend realization using xarray for data handling."""

import xarray as xr

from aqua.core.data_model import DataModel
from aqua.core.fixer import Fixer

from .backend import Backend


class BackendXarray(Backend):
    """
    Concrete retriever that opens data directly with xarray from a path.
    No intake catalog is involved. Supports netCDF and zarr.
    """

    SUPPORTED_ENGINES = ("netcdf4", "zarr")

    def __init__(
        self,
        path: str,
        xarray_engine: str = None,
        chunks: str | dict = "auto",
        fixer: Fixer = None,
        datamodel: DataModel = None,
        loglevel: str = "WARNING",
        **kwargs,
    ):
        """
        Initialize the BackendXarray instance.

        Args:
            path (str): Path to the data file or directory.
            xarray_engine (str, optional): Engine to use for opening the data file. If None, it will be detected automatically.
            chunks (str | dict, optional): Chunking strategy for xarray. Defaults to "auto".
            fixer (Fixer, optional): An instance of Fixer to apply data fixes. Defaults to None.
            datamodel (DataModel, optional): An instance of DataModel to define the data structure. Defaults to None.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            kwargs: Additional keyword arguments to pass to xarray's open_dataset or open_zarr functions.
        """

        super().__init__(fixer=fixer, datamodel=datamodel, loglevel=loglevel)
        detected_engine = self._detect_engine(path)
        xarray_engine = xarray_engine or detected_engine

        if xarray_engine not in self.SUPPORTED_ENGINES:
            raise ValueError(f"xarray_engine must be one of {self.SUPPORTED_ENGINES}, got {xarray_engine!r}")
        self.path = path
        self.engine = xarray_engine
        self.chunks = chunks
        # TODO: method to validate xarray kwargs
        # self.xr_kwargs = new_kwargs

    def retrieve_plain(self, startdate: str = None):
        pass

    def retrieve(
        self,
        var: str | list = None,
        level: str | list = None,
        level_coord: str = None,
        startdate: str = None,
        enddate: str = None,
    ):
        # TODO: Add kwargs to pass to xarray open_dataset or open_zarr
        data = xr.open_mfdataset(self.path, chunks=self.chunks, engine=self.engine)

        data = super()._postprocess_data(
            data=data,
            var=var,
            level=level,
            level_coord=level_coord,
            startdate=startdate,
            enddate=enddate,
        )

        return data

    def _seldate(self, data: xr.Dataset, startdate: str = None, enddate: str = None):
        return super()._seldate(data=data, startdate=startdate, enddate=enddate)

    def _sellevel(self, data: xr.Dataset, level: str | list = None, level_coord: str = None):
        return super()._sellevel(data=data, level=level, level_coord=level_coord)

    def _selvar(
        self,
        data: xr.Dataset,
        var: str | list = None,
    ):
        return super()._selvar(data=data, var=var)

    @staticmethod
    def _detect_engine(path):
        """
        Detect the engine of the data file based on its extension.

        Args:
            path (str): Path to the data file or directory.
        """
        # TODO: expand with glob to detect that are also all files of the same format
        # Raise NoDataError if the expanded glob does not contain any files of the same format
        if path.endswith(".zarr"):
            return "zarr"
        if path.endswith(".nc") or path.endswith(".nc4"):
            return "netcdf4"
        else:
            raise ValueError(f"Could not detect format from path: {path}")

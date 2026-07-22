import xarray as xr
from intake.source import base

from .readers import NetCDFZarrDatasetReader


class IntakeXarraySourceAdapter(base.DataSource):
    container = "xarray"
    name = "xarray"
    version = ""

    def __init__(self, data, xarray_kwargs=None, metadata=None, reader_class=NetCDFZarrDatasetReader, **kwargs):
        """Build the intake 2 reader and expose the attributes the AQUA backend needs.

        Args:
            data (intake.readers.BaseData): The datatype the reader reads from.
            xarray_kwargs (dict, optional): Kwargs for the xarray open call.
            metadata (dict, optional): Catalog metadata for this source.
            reader_class (type, optional): Reader to build. Defaults to NetCDFZarrDatasetReader.
            kwargs: Further parameters forwarded to the reader (e.g. chunks).
        """
        xarray_kwargs = dict(xarray_kwargs or {})
        # 'use_cftime' is deprecated as an xarray kwarg (and a TypeError together with a
        # decode_times coder), but legacy catalog entries still carry it: fold it into a coder
        if "use_cftime" in xarray_kwargs:
            xarray_kwargs["decode_times"] = xr.coders.CFDatetimeCoder(use_cftime=xarray_kwargs.pop("use_cftime"))

        self.xarray_kwargs = xarray_kwargs
        self.data = data
        self.reader = reader_class(data, **xarray_kwargs, metadata=metadata, **kwargs)
        super().__init__(metadata=metadata)

    def to_dask(self):
        if "chunks" not in self.reader.kwargs:
            return self.reader(chunks={}).read()
        else:
            return self.reader.read()

    def __call__(self, *args, **kwargs):
        return self

    get = __call__

    def read(self):
        return self.reader(chunks=None).read()

    discover = read

    read_chunked = to_dask

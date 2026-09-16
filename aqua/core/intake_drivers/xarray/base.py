from intake.source import base


class IntakeXarraySourceAdapter(base.DataSource):
    container = "xarray"
    name = "xarray"
    version = ""

    def __init__(self, data, xarray_kwargs=None, metadata=None):
        """Expose the attributes the AQUA backend reads the source through.

        The concrete sources build ``self.reader`` themselves, then call this.

        Args:
            data (intake.readers.BaseData): The datatype the reader reads from; the backend
                narrows its ``url`` in place (glob expansion, date filtering).
            xarray_kwargs (dict, optional): The effective kwargs of the xarray open call.
            metadata (dict, optional): Catalog metadata for this source, set as ``self.metadata``.
        """
        self.data = data
        self.xarray_kwargs = xarray_kwargs or {}
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

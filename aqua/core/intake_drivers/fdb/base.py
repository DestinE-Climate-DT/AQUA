from intake.source import base


class IntakeFDBSourceAdapter(base.DataSource):
    container = "fdb"
    name = "fdb"
    version = ""

    def to_dask(self):
        return self.reader.read()

    def __call__(self, *args, **kwargs):
        return self

    get = __call__

    def read(self):
        return self.reader(chunks=None).read()

    discover = read

    read_chunked = to_dask

from intake import BaseReader

from .datatypes import Icechunk
from .openers import open_icechunk


class IcechunkDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {"icechunk", "xarray"}
    optional_imports = {}
    func = "open_icechunk"
    implements = {
        Icechunk,
    }

    # def to_dask(self):
    #     return self.reader.read()

    # def __call__(self, *args, **kwargs):
    #     return self

    # get = __call__

    # def read(self):
    #     return self.reader(chunks=None).read()

    # discover = read

    # read_chunked = to_dask

    def _read(self, data, **kwargs):
        params = data.to_dict()
        params.update(kwargs)
        return open_icechunk(**params)

from intake import BaseReader

from .datatypes import GSV, Polytope, Z3FDB
from .openers import open_gsv, open_polytope, open_z3fdb


class GSVDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {}
    optional_imports = {}
    func = "open_gsv"
    implements = {
        GSV,
    }
    partition_access = True

    def _read(self, data, **kwargs):

        params = data.to_dict()
        params.update(kwargs)
        return open_gsv(data.request, **params)


class PolytopeDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {}
    optional_imports = {}
    func = "open_polytope"
    implements = {
        Polytope,
    }
    partition_access = True

    def _read(self, data, **kwargs):

        params = data.to_dict()
        params.update(kwargs)
        return open_polytope(data.request, **params)


class Z3FDBDatasetReader(BaseReader):
    output_instance = "xarray:Dataset"
    imports = {}
    optional_imports = {}
    func = "open_z3fdb"
    implements = {
        Polytope,
    }
    partition_access = True

    def _read(self, data, **kwargs):

        return open_z3fdb(
            data.request,
            startdate=data.startdate,
            enddate=data.enddate,
            config="/home/jvonhar/work/AQUA/aqua/core/intake_drivers/fdb/z3fdb/config-z3fdb.yaml",
            variables=data.var,
            levels=data.level,
            freq=data.savefreq)

# ds = open_z3fdb(
#     request=request3d_dict,
#     # years=range(1990, 2014).
#     start_date="1990-01-01", end_date="1992-12-31",
#     config="/home/jvonhar/work/AQUA/aqua/core/intake_drivers/fdb/z3fdb/config-z3fdb.yaml",
#     variables = variables,
#     levels=[400, 300],
#     freq="MS"

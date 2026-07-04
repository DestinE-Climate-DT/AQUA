import os

from intake import BaseReader

from aqua.core.configurer import ConfigPath

from .datatypes import GSV, Z3FDB, Polytope
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
        Z3FDB,
    }
    partition_access = True

    def _read(self, data, **kwargs):

        config_z3fdb = os.path.join(ConfigPath().configdir, "config-z3fdb.yaml")
        config_z3fdb = kwargs.pop("config_z3fdb", config_z3fdb)

        return open_z3fdb(
            data.request,
            startdate=data.startdate,
            enddate=data.enddate,
            config=config_z3fdb,
            variables=data.var,
            levels=data.level,
            freq=data.savefreq,
            data_start_date=data.data_start_date,
            data_end_date=data.data_end_date,
        )

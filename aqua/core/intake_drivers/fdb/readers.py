import os

from intake import BaseReader

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

        # Establish wehere to find the config.yaml file in this order:
        # 1. if config_fdb passed as a kwarg use that.
        # 2. if data.metadata has a key fdb_home_bridge use that.
        # 3. if FDB_HOME_BRIDGE set as an environment variable use that.

        config_fdb = None
        if "config_fdb" in kwargs:
            config_fdb = kwargs.pop("config_fdb")
        elif "fdb_home_bridge" in data.metadata and (
            data.bridge_start_date == "complete" or data.bridge_end_date == "complete"
        ):
            config_fdb = data.metadata.pop("fdb_home_bridge")
            config_fdb = config_fdb if config_fdb.endswith("yaml") else os.path.join(config_fdb, "etc/fdb/config.yaml")
        else:
            raise ValueError(
                "Could not find FDB config.yaml. Please pass it as a kwarg or set the key fdb_home_bridge in the catalog."
            )

        return open_z3fdb(
            data.request,
            startdate=data.startdate,
            enddate=data.enddate,
            config=config_fdb,
            variables=data.var,
            levels=data.level,
            freq=data.savefreq,
            data_start_date=data.data_start_date,
            data_end_date=data.data_end_date,
            chunks=data.chunks,
            level_values=data.level_values,
        )

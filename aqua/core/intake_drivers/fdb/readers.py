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
        # 2. if data.metadata has a key fdb_home_bridge or fdb_path_bridge use that.

        config_fdb = None
        if data.config_fdb:
            config_fdb = data.config_fdb
        elif data.bridge_start_date or data.bridge_end_date:
            if "fdb_home_bridge" in data.metadata:
                config_fdb = data.metadata.pop("fdb_home_bridge")
                config_fdb = os.path.join(config_fdb, "etc/fdb/config.yaml")
            elif "fdb_path_bridge" in data.metadata:
                config_fdb = data.metadata.pop("fdb_path_bridge")
        else:
            if "fdb_home" in data.metadata:
                config_fdb = data.metadata.pop("fdb_home")
                config_fdb = os.path.join(config_fdb, "etc/fdb/config.yaml")
            elif "fdb_path" in data.metadata:
                config_fdb = data.metadata.pop("fdb_path")

        # With z3fdb we can apparently only read data from the bridge
        if not data.enddate and data.bridge_end_date and data.bridge_end_date != "complete":
            data.data_end_date = data.bridge_end_date

        if not data.startdate and data.bridge_start_date and data.bridge_start_date != "complete":
            data.data_start_date = data.bridge_start_date

        if "source_grid_name" in data.metadata:
            grid = data.metadata.pop("source_grid_name")
        else:
            grid = None

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
            grid=grid
        )

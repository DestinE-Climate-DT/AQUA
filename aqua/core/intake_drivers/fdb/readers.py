import os
import pandas as pd
import xarray as xr

from intake import BaseReader

from .datatypes import GSV, Z3FDB, Polytope
from .openers import open_gsv, open_polytope, open_z3fdb

xr.set_options(keep_attrs=True)


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

        # Pop keys from metadata to keep behavior consistent and extract configuration values
        hpc_home = data.metadata.pop("fdb_home", None)
        hpc_path = data.metadata.pop("fdb_path", None)
        bridge_home = data.metadata.pop("fdb_home_bridge", None)
        bridge_path = data.metadata.pop("fdb_path_bridge", None)

        if "source_grid_name" in data.metadata:
            grid = data.metadata.pop("source_grid_name")
        else:
            grid = None

        config_hpc = None
        config_bridge = None

        if data.config_fdb:
            config_hpc = data.config_fdb
            config_bridge = data.config_fdb
        else:
            if hpc_home:
                config_hpc = os.path.join(hpc_home, "etc/fdb/config.yaml")
            elif hpc_path:
                config_hpc = hpc_path

            if bridge_home:
                config_bridge = os.path.join(bridge_home, "etc/fdb/config.yaml")
            elif bridge_path:
                config_bridge = bridge_path
            else:
                config_bridge = config_hpc

        # Determine overall dates to read
        effective_start = data.startdate if data.startdate is not None else data.data_start_date
        effective_end = data.enddate if data.enddate is not None else data.data_end_date

        start_ts = pd.Timestamp(str(effective_start))
        end_ts = pd.Timestamp(str(effective_end))

        has_bridge = (data.bridge_start_date is not None) or (data.bridge_end_date is not None)

        periods = []

        if has_bridge:
            def get_bridge_date(val, default_val):
                if val is None or val == "complete":
                    return pd.Timestamp(str(default_val))
                return pd.Timestamp(str(val))

            b_start_ts = get_bridge_date(data.bridge_start_date, data.data_start_date)
            b_end_ts = get_bridge_date(data.bridge_end_date, data.data_end_date)

            # Normalize frequency offset
            freq = data.savefreq
            if freq == "h":
                offset_freq = "1h"
            elif freq == "D":
                offset_freq = "1D"
            else:
                offset_freq = freq

            try:
                offset = pd.to_timedelta(offset_freq)
            except ValueError:
                if offset_freq in ("MS", "M"):
                    offset = pd.DateOffset(months=1)
                else:
                    offset = pd.Timedelta(days=1)

            # Define intersection of bridge with requested range
            bridge_intersect_start = max(start_ts, b_start_ts)
            bridge_intersect_end = min(end_ts, b_end_ts)

            # 1. Before bridge: [start_ts, bridge_intersect_start - offset]
            if start_ts < b_start_ts:
                before_end = min(end_ts, b_start_ts - offset)
                if start_ts <= before_end:
                    periods.append((start_ts, before_end, config_hpc))

            # 2. Central bridge: [bridge_intersect_start, bridge_intersect_end]
            if bridge_intersect_start <= bridge_intersect_end:
                periods.append((bridge_intersect_start, bridge_intersect_end, config_bridge))

            # 3. After bridge: [bridge_intersect_end + offset, end_ts]
            if end_ts > b_end_ts:
                after_start = max(start_ts, b_end_ts + offset)
                if after_start <= end_ts:
                    periods.append((after_start, end_ts, config_hpc))
        else:
            periods.append((start_ts, end_ts, config_hpc))

        datasets = []
        for p_start, p_end, p_config in periods:
            p_start_str = p_start.strftime("%Y%m%dT%H%M")
            p_end_str = p_end.strftime("%Y%m%dT%H%M")

            ds = open_z3fdb(
                data.request,
                startdate=p_start_str,
                enddate=p_end_str,
                config=p_config,
                variables=data.var,
                levels=data.level,
                freq=data.savefreq,
                data_start_date=p_start_str,
                data_end_date=p_end_str,
                chunks=data.chunks,
                level_values=data.level_values,
                grid=grid
            )
            datasets.append(ds)

        if not datasets:
            raise ValueError("No valid date periods determined for reading.")

        if len(datasets) == 1:
            return datasets[0]

        combined = xr.concat(datasets, dim="time")
        combined.attrs.update(datasets[0].attrs)
        return combined

import intake
import pyodc as odc
import pandas as pd
import xarray as xr
import glob
import dask
from aqua.logger import log_configure
import dask.dataframe as dd

class ODBSource(intake.source.base.DataSource):
    """
    Intake driver for ECMWF ODB-2 files using pyodc.
    Returns an xarray.Dataset with automatic `time` dimension if
    year/month/day/hour[/minute/second] columns are present.
    """
    container = "xarray"
    name = "odb"
    version = "0.0.1"
    partition_access = True

    _ds = None # _ds and _da will contain samples of the data for dask access
    _da = None
    dask_access = False  # Flag to indicate if dask access is enabled
    first_run = True  # Flag to check if this is the first run of the class

    def __init__(self, paths, columns=None, single=True, loglevel='WARNING', **kwargs):
        """
        Initializes the ODBSource class.

        Args:
            paths (str or list of str): Paths to the ODB files.
            columns (list of str, optional): Columns to read from the ODB files.
            single (bool, optional): If True, read a single file; if False, read multiple files.
            variable (str, optional): Name of the variable to promote. Default is 'auto'.
            loglevel (str, optional): Logging level to use. Default is 'WARNING'.
        """
        self.logger = log_configure(log_level=loglevel, log_name='ODBSource')
        self.logger.debug("Initializing ODBSource with paths: %s", paths)

        self.paths = self._format_paths(paths)
        self.columns = columns
        self.single = single

        ODBSource.first_run = False

        super(ODBSource, self).__init__(**kwargs)

    def _get_schema(self):
        """
        Standard method providing data schema.
        """
        if self.dask_access:
            if not self._ds or not self._da:
                # Load a sample to get the schema
                self._ds = self._get_partition(0)
                self.logger.debug("Loaded sample dataset for schema")
                var = list(self._ds.data_vars)[0]
                self._da = self._ds[var]
                self.logger.debug("Loaded sample dataarray for schema")
            schema = intake.source.base.Schema(
                datashape=None,
                dtype=str(self._da.dtype),
                shape=self._da.shape,
                name=None,
                partitions=self._npartitions
            )
        else:
            schema = intake.source.base.Schema(
                datashape=None,
                dtype=str(xr.Dataset),
                shape=None,
                name=None,
                partitions=self._npartitions
            )

        return schema

    def _to_xarray(self, df: pd.DataFrame) -> xr.Dataset:
        """
        Converts an ODB pandas DataFrame into an xarray.Dataset.
        Uses self.variable: 
            - "auto": Promote all `@body` columns to data variables.
            - str: Promote the `@body` column with the specified name.

        Args:
            df (pd.DataFrame): One partition of ODB data.
            

        Returns:
            xr.Dataset: The resulting xarray.Dataset.
        """
        # --- Decide variables vs metadata ---
        var_cols = [c for c in df.columns if c.endswith("@body")]
        self.logger.debug("Variable columns identified: %s", var_cols)

        # Everything else = coordinates / metadata
        coord_cols = [c for c in df.columns if c not in var_cols]
        self.logger.debug("Coordinate colums: %s", coord_cols)

        # Remove @hdr from any coor_cols
        coord_cols_old = coord_cols
        coord_cols = [c.replace("@hdr", "") for c in coord_cols]
        # Remove if from the column names
        df.columns = [c.replace("@hdr", "") for c in df.columns]

        # Filter out metadata
        metadata_var = ['latitude', 'longitude', 'elevation', 'station']
        attributes = {}
        for md in metadata_var:
            if md in coord_cols:
                coord_cols.remove(md)
                attributes[md] = df[md].values[0]
                df.drop(md, axis=1, inplace=True)
                self.logger.debug("Removed metadata column: %s", md)

        # --- Build Dataset ---
        ds = xr.Dataset()

        # Make time the main dimension if available
        if "time" in df.columns:
            ds = ds.assign_coords(time=("index", df["time"].values))
            ds = ds.swap_dims({"index": "time"})

        # Coordinates
        for c in coord_cols:
            ds = ds.assign_coords({c: ("time", df[c].values)})

        # Variables
        for v in var_cols:
            ds[v] = ("time", df[v].values)

        # Add attributes
        ds.attrs.update(attributes)

        return ds

    def _get_partition(self, i):
        """
        Standard internal method reading i-th data partition from FDB

        Args:
            i (int): partition number

        Returns:
            xr.Dataset: xarray.Dataset for the requested partition
        """
        df = odc.read_odb(self.paths[i], single=self.single, columns=self.columns)
        # --- Build time coordinate if possible ---
        time_map = {
            "year@hdr": "year",
            "month@hdr": "month",
            "day@hdr": "day",
            "hour@hdr": "hour",
            "minute@hdr": "minute",
            "second@hdr": "second",
        }

        available = {alias: df[col]
                     for col, alias in time_map.items()
                     if col in df.columns}

        if {"year", "month", "day", "hour"} <= set(available.keys()):
            df = df.copy()
            df["time"] = pd.to_datetime(available, errors="coerce")

            # Drop original columns
            df = df.drop(columns=[c for c in time_map if c in df.columns])

        # --- Convert to xarray ---
        ds = self._to_xarray(df)

        return ds

    def read(self):
        parts = [self._get_partition(i) for i in range(len(self.paths))]
        return xr.concat(parts, dim="time") if len(parts) > 1 else parts[0]

    def get_part_delayed(self, i, shape, dtype):
        """
        Get a delayed xarray dataset for the i-th partition.

        Args:
            i (int): partition number
            shape: shape of the schema
            dtype: dtype of the schema

        Returns:
            dask.array.Array: delayed xarray dataset for the requested partition
        """
        ds = dask.delayed(self._get_partition)(i)
        ds = ds.to_array()[0].data

        newshape = list(shape)
        return dask.array.from_delayed(ds, newshape, dtype)

    def to_dask(self):
        self.dask_access = True
        _ = self.discover()

        # # Each partition delayed
        # delayed_dsets = [dask.delayed(self._get_partition)(i) for i in range(len(self.paths))]

        # # Trigger concat lazily
        # ds = xr.concat(
        #     [xr.Dataset.from_dataframe(dask.compute(d.to_dataframe())[0]) for d in delayed_dsets],
        #     dim="index"
        # )

        ds = self._get_partition(0)

        return ds

    def _format_paths(self, paths):
        """
        Format the paths so that they are always a list.
        Additionally if a wildcard is present, it will be expanded.
        """

        if isinstance(paths, str):
            paths = [paths]

        # Expand wildcards in paths
        expanded_paths = []
        for path in paths:
            expanded_paths.extend(glob.glob(path))

        self._npartitions = len(expanded_paths)

        return expanded_paths
    
    # def _to_xarray(self, df: pd.DataFrame) -> xr.Dataset:
    #     # build time first (like you already do)

    #     if {"station@hdr", "time"}.issubset(df.columns):
    #         # pivot so rows = time, cols = station
    #         df_pivot = df.pivot(index="time", columns="station@hdr", values="value@body")

    #         ds = xr.Dataset(
    #             {"value": (("index", "station"), df_pivot.values)},
    #             coords={
    #                 "index": df_pivot.index.values,
    #                 "station": df_pivot.columns.values,
    #             }
    #         )

    #         # Add station lon/lat as coordinates
    #         if {"longitude@hdr", "latitude@hdr"}.issubset(df.columns):
    #             station_meta = df[["station@hdr", "longitude@hdr", "latitude@hdr"]].drop_duplicates("station@hdr")
    #             station_meta = station_meta.set_index("station@hdr").loc[df_pivot.columns]

    #             ds = ds.assign_coords({
    #                 "lon": ("station", station_meta["longitude@hdr"].values),
    #                 "lat": ("station", station_meta["latitude@hdr"].values),
    #             })
    #     else:
    #         # fallback to flat Dataset
    #         ds = xr.Dataset.from_dataframe(df)

    #     return ds
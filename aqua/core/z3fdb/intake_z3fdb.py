"""An intake driver for z3fdb access.

z3fdb provides a virtual Zarr store backed by MARS/FDB GRIB data.  A
``SimpleStoreBuilder`` assembles one or more MARS request *parts*, each
annotated with ``AxisDefinition`` objects that map MARS keys (date, time,
param, levelist, …) onto Zarr dimensions.  This driver wraps that API in the
standard intake ``DataSource`` contract, exposes lazy dask-backed
``xarray.Dataset`` objects and plugs into AQUA's ``Reader`` pipeline exactly
like the existing ``GSVSource`` driver.

Example catalog entry::

    sources:
      era5_surface:
        driver: z3fdb
        args:
          data_start_date: "2020-01-01"
          data_end_date:   "2020-12-31"
          chunks: "D"           # one calendar day per dask partition
          parts:
            - request: >-
                type=an,class=ea,domain=g,expver=0001,stream=oper,
                date=2020-01-01/2020-01-02,levtype=sfc,step=0,
                param=165/166,time=0/to/21/by/3
              axes:
                - keys: [date, time]   # Dim 0 — 2 dates × 8 times = 16
                - keys: [param]        # Dim 1 — 2 params
        metadata:
          fdb_path: /path/to/fdb5_config.yaml
          variables: [165, 166]
          source_grid_name: N320

Multi-part example (surface + pressure levels concatenated on axis 1)::

    sources:
      era5_3d:
        driver: z3fdb
        args:
          data_start_date: "2020-01-01"
          data_end_date:   "2020-12-31"
          extend_on_axis: 1
          parts:
            - request: >-
                type=an,class=ea,domain=g,expver=0001,stream=oper,
                date=2020-01-01/2020-01-02,levtype=sfc,step=0,
                param=165/166,time=0/to/21/by/3
              axes:
                - keys: [date, time]
                - keys: [param]
            - request: >-
                type=an,class=ea,domain=g,expver=0001,stream=oper,
                date=2020-01-01/2020-01-02,levtype=pl,step=0,
                param=131/132,levelist=50/100,time=0/to/21/by/3
              axes:
                - keys: [date, time]
                - keys: [param, levelist]
        metadata:
          fdb_path: /path/to/fdb5_config.yaml
          variables: [165, 166, 131, 132]
          source_grid_name: N320
"""

import os
import re
from itertools import product

import dask
import dask.array
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from intake.source import base

from aqua.core.logger import log_configure, log_history
from aqua.core.util import to_list
from aqua.core.util.eccodes import get_eccodes_attr

xr.set_options(keep_attrs=True)

# ---------------------------------------------------------------------------
# Optional z3fdb import
# ---------------------------------------------------------------------------

try:
    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

    z3fdb_available = True
except ImportError:
    z3fdb_available = False
    z3fdb_error_cause = (
        "z3fdb package is not installed. "
        "Install it with: pip install z3fdb"
    )

# ---------------------------------------------------------------------------
# MARS request string helpers
# ---------------------------------------------------------------------------


def _parse_mars_request(request_str):
    """Parse a MARS request string into a ``{key: raw_value_str}`` dict.

    Whitespace and embedded newlines are stripped so that multi-line YAML
    scalars are handled transparently.

    Args:
        request_str (str): MARS request string, e.g.
            ``"type=an,class=ea,date=2020-01-01/2020-01-02,param=165/166"``.

    Returns:
        dict: Mapping of MARS key to raw value string.
    """
    result = {}
    # collapse whitespace / newlines that YAML multi-line scalars may introduce
    clean = re.sub(r"\s+", "", request_str)
    for token in clean.split(","):
        if "=" in token:
            key, val = token.split("=", 1)
            result[key] = val
    return result


def _expand_mars_value(val_str):
    """Expand a MARS value expression into a flat list of strings.

    Supports:
    - Simple slash-separated list: ``"165/166"`` → ``["165", "166"]``
    - Range notation: ``"0/to/21/by/3"`` → ``["0", "3", …, "21"]``
    - Prefix values before ``/to/``: ``"0/6/to/18/by/6"`` → ``["0","6","12","18"]``

    Args:
        val_str (str): Raw MARS value string.

    Returns:
        list[str]: Expanded list of value strings.
    """
    if not val_str:
        return []
    parts = [p.strip() for p in val_str.split("/")]
    if "to" not in parts:
        return parts

    to_idx = parts.index("to")
    by_idx = parts.index("by") if "by" in parts else None

    prefix = parts[: to_idx - 1]  # values before the range start
    start = float(parts[to_idx - 1])
    end = float(parts[to_idx + 1])
    step = float(parts[by_idx + 1]) if by_idx is not None else 1.0

    expanded = []
    v = start
    while v <= end + step * 1e-9:  # small epsilon to include end
        expanded.append(str(int(v)) if v == int(v) else str(v))
        v += step

    return prefix + expanded


def _build_time_index(dates, times):
    """Build a ``pd.DatetimeIndex`` from lists of ISO-date and HHMM strings.

    Args:
        dates (list[str]): ISO-format date strings, e.g. ``["2020-01-01"]``.
        times (list[str]): Time strings in ``HHMM`` or ``HH`` format, e.g.
            ``["0000", "0600", "1200", "1800"]``.

    Returns:
        pd.DatetimeIndex: Combined timestamps; *time* varies fastest.
    """
    timestamps = []
    for d, t in product(dates, times):
        t_norm = t.zfill(4)  # ensure HHMM
        timestamps.append(pd.Timestamp(f"{d}T{t_norm[:2]}:{t_norm[2:]}"))
    return pd.DatetimeIndex(timestamps)


# ---------------------------------------------------------------------------
# Main intake source class
# ---------------------------------------------------------------------------


class Z3FDBSource(base.DataSource):
    """Intake driver that reads GRIB data from FDB via a z3fdb Zarr store.

    The driver wraps ``z3fdb.SimpleStoreBuilder`` to create a virtual Zarr
    array for each time partition, then converts it to an ``xarray.Dataset``
    with proper time, (optional) level, and spatial coordinates.

    Args:
        parts (list[dict]): Ordered list of part definitions.  Each element
            must contain:

            - ``request`` (str): MARS request string.
            - ``axes`` (list[dict]): Axis definitions.  Each dict has a
              ``keys`` field (list of MARS key names, e.g. ``["date","time"]``)
              and an optional ``chunking`` field (default ``"SINGLE_VALUE"``).

        data_start_date (str): First available date (ISO format).
        data_end_date (str): Last available date (ISO format).
        extend_on_axis (int, optional): Zarr axis on which multi-part stores
            are concatenated (maps to ``builder.extend_on_axis(N)``).
            Defaults to ``None`` (single-part stores only).
        chunks (str, optional): Pandas frequency alias controlling the size of
            each dask partition (e.g. ``"D"`` for daily, ``"M"`` for monthly).
            Defaults to ``"D"``.
        startdate (str, optional): First date to read.  Defaults to
            ``data_start_date``.
        enddate (str, optional): Last date to read.  Defaults to
            ``data_end_date``.
        var (list, optional): Param IDs to expose.  Overrides
            ``metadata["variables"]``.
        metadata (dict, optional): Catalog metadata.  Recognised keys:

            - ``fdb_path`` (str): FDB5 config file path
              (``FDB5_CONFIG_FILE``).
            - ``fdb_home`` (str): FDB home directory (``FDB_HOME``).
            - ``variables`` (list): Default param ID list.
            - ``source_grid_name`` (str): Grid identifier for AQUA.

        loglevel (str, optional): Logging level.  Defaults to ``"WARNING"``.
        **kwargs: Forwarded to the ``intake`` base class.
    """

    container = "xarray"
    name = "z3fdb"
    version = "0.0.1"
    partition_access = True

    # cached sample for schema inspection
    _sample_ds = None

    def __init__(
        self,
        parts,
        data_start_date,
        data_end_date,
        extend_on_axis=None,
        chunks="D",
        startdate=None,
        enddate=None,
        var=None,
        metadata=None,
        loglevel="WARNING",
        **kwargs,
    ):
        if not z3fdb_available:
            raise ImportError(z3fdb_error_cause)

        self.logger = log_configure(log_level=loglevel, log_name="Z3FDBSource")
        self.loglevel = loglevel

        self.parts = parts
        self.extend_on_axis = extend_on_axis
        self.chunks_freq = chunks

        # FDB environment paths from catalog metadata
        if metadata:
            self.fdb_path = metadata.get("fdb_path", None)
            self.fdb_home = metadata.get("fdb_home", None)
        else:
            self.fdb_path = None
            self.fdb_home = None

        # Variable list (param IDs)
        default_vars = metadata.get("variables", []) if metadata else []
        self._var = to_list(var) if var is not None else to_list(default_vars)

        # Date range
        self.data_start_date = pd.Timestamp(data_start_date)
        self.data_end_date = pd.Timestamp(data_end_date)
        self.startdate = pd.Timestamp(startdate) if startdate else self.data_start_date
        self.enddate = pd.Timestamp(enddate) if enddate else self.data_end_date

        # Build the list of (chunk_start, chunk_end) pairs
        self._time_partitions = self._build_time_partitions()
        self._npartitions = len(self._time_partitions)

        self.logger.debug(
            "Z3FDBSource: %d partitions from %s to %s (freq=%s)",
            self._npartitions,
            self.startdate,
            self.enddate,
            self.chunks_freq,
        )

        super().__init__(metadata=metadata)

    # ------------------------------------------------------------------
    # Pickling support (required for dask serialisation across workers)
    # ------------------------------------------------------------------

    def __getstate__(self):
        return {
            "parts": self.parts,
            "extend_on_axis": self.extend_on_axis,
            "chunks_freq": self.chunks_freq,
            "fdb_path": self.fdb_path,
            "fdb_home": self.fdb_home,
            "_var": self._var,
            "data_start_date": self.data_start_date,
            "data_end_date": self.data_end_date,
            "startdate": self.startdate,
            "enddate": self.enddate,
            "_time_partitions": self._time_partitions,
            "_npartitions": self._npartitions,
            "loglevel": self.loglevel,
            "logger": self.logger,
        }

    def __setstate__(self, state):
        for key, val in state.items():
            setattr(self, key, val)

    # ------------------------------------------------------------------
    # Time partitioning
    # ------------------------------------------------------------------

    def _build_time_partitions(self):
        """Partition the requested date range into chunk-sized intervals.

        Returns:
            list[tuple[pd.Timestamp, pd.Timestamp]]: ``(start, end)`` pairs,
            one per dask partition.
        """
        date_range = pd.date_range(self.startdate, self.enddate, freq=self.chunks_freq)
        if len(date_range) == 0:
            return [(self.startdate, self.enddate)]

        partitions = []
        for i, t_start in enumerate(date_range):
            t_end = (
                date_range[i + 1] - pd.Timedelta("1s")
                if i + 1 < len(date_range)
                else self.enddate
            )
            partitions.append((t_start, min(t_end, self.enddate)))
        return partitions

    def _get_chunk_dates(self, chunk_start, chunk_end):
        """Return ISO date strings for every calendar day in [chunk_start, chunk_end].

        Args:
            chunk_start (pd.Timestamp): Start of the chunk.
            chunk_end (pd.Timestamp): End of the chunk.

        Returns:
            list[str]: ISO date strings.
        """
        return [
            d.strftime("%Y-%m-%d")
            for d in pd.date_range(chunk_start.date(), chunk_end.date(), freq="D")
        ]

    def _build_time_coords(self, chunk_start, chunk_end):
        """Build a ``pd.DatetimeIndex`` for a given chunk.

        Uses the ``date`` and ``time`` fields of the first part's request to
        enumerate all timestamps in the chunk.

        Args:
            chunk_start (pd.Timestamp): Start of the chunk.
            chunk_end (pd.Timestamp): End of the chunk.

        Returns:
            pd.DatetimeIndex
        """
        first_req = _parse_mars_request(self.parts[0]["request"])
        first_axes = self.parts[0]["axes"]
        time_keys = first_axes[0]["keys"]

        dates = self._get_chunk_dates(chunk_start, chunk_end)

        if "time" in time_keys:
            raw_times = _expand_mars_value(first_req.get("time", "0000"))
            # Normalise to 4-char HHMM
            times = [t.zfill(4) if len(t) <= 4 else t for t in raw_times]
            return _build_time_index(dates, times)

        return pd.DatetimeIndex([pd.Timestamp(d) for d in dates])

    # ------------------------------------------------------------------
    # Request string manipulation
    # ------------------------------------------------------------------

    @staticmethod
    def _set_dates_in_request(request_str, dates):
        """Replace the ``date`` field in a MARS request string.

        Args:
            request_str (str): Original MARS request string.
            dates (list[str]): ISO date strings for the chunk.

        Returns:
            str: Updated MARS request string.
        """
        date_val = "/".join(dates)
        return re.sub(r"date=[^,]+", f"date={date_val}", request_str)

    # ------------------------------------------------------------------
    # Zarr store construction
    # ------------------------------------------------------------------

    def _set_fdb_env(self):
        """Set FDB environment variables from catalog metadata if present."""
        if self.fdb_home:
            os.environ["FDB_HOME"] = self.fdb_home
            self.logger.debug("FDB_HOME set to %s", self.fdb_home)
        if self.fdb_path:
            os.environ["FDB5_CONFIG_FILE"] = self.fdb_path
            self.logger.debug("FDB5_CONFIG_FILE set to %s", self.fdb_path)

    def _build_store(self, chunk_start, chunk_end):
        """Build a z3fdb Zarr store for the given date range.

        Args:
            chunk_start (pd.Timestamp): Start of the time chunk.
            chunk_end (pd.Timestamp): End of the time chunk.

        Returns:
            zarr store object compatible with ``zarr.open_array``.
        """
        self._set_fdb_env()
        dates = self._get_chunk_dates(chunk_start, chunk_end)
        self.logger.debug("Building z3fdb store for dates: %s", dates)

        builder = SimpleStoreBuilder()
        for part in self.parts:
            request_str = self._set_dates_in_request(part["request"], dates)
            axes = [
                AxisDefinition(ax["keys"], Chunking.SINGLE_VALUE)
                for ax in part["axes"]
            ]
            builder.add_part(request_str, axes, ExtractorType.GRIB)

        if self.extend_on_axis is not None:
            builder.extend_on_axis(self.extend_on_axis)

        return builder.build()

    # ------------------------------------------------------------------
    # Zarr-to-xarray conversion
    # ------------------------------------------------------------------

    def _zarr_to_xarray(self, store, chunk_start, chunk_end):
        """Convert a z3fdb Zarr store to an ``xarray.Dataset``.

        The Zarr array produced by z3fdb has the layout::

            data[time_idx, param_idx, grid_idx]

        where:
        - ``time_idx`` walks the ``date × time`` product (time varies fastest),
        - ``param_idx`` walks either the ``param`` list (surface) or the
          ``param × levelist`` product (pressure-level); for multi-part stores
          this axis is the concatenation across parts,
        - ``grid_idx`` is a flat array of grid-point values.

        This method reconstructs named coordinates from the catalog's axis
        definitions and returns a tidy ``xr.Dataset``.

        Args:
            store: z3fdb zarr store.
            chunk_start (pd.Timestamp): Chunk start (for coordinate labels).
            chunk_end (pd.Timestamp): Chunk end.

        Returns:
            xr.Dataset
        """
        zarr_array = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)
        self.logger.debug("Zarr array shape: %s", zarr_array.shape)

        time_index = self.build_time_coords(chunk_start, chunk_end)
        n_time = len(time_index)

        ds = xr.Dataset()
        param_offset = 0  # running column offset across parts

        for part in self.parts:
            part_req = _parse_mars_request(part["request"])

            # Identify param and (optional) level axes from the catalog definition
            param_axis_keys = part["axes"][1]["keys"] if len(part["axes"]) > 1 else ["param"]

            params = _expand_mars_value(part_req.get("param", ""))
            has_levels = "levelist" in param_axis_keys
            levels = _expand_mars_value(part_req.get("levelist", "")) if has_levels else []

            for p_idx, param in enumerate(params):
                param_id = int(param)
                try:
                    short_name = get_eccodes_attr(param_id)["shortName"]
                except Exception:
                    self.logger.warning("Could not resolve eccodes shortName for paramId %d", param_id)
                    short_name = f"param{param_id}"

                if has_levels:
                    # Each (param, level) pair occupies one column: param varies slowest
                    level_arrays = []
                    for l_idx, level in enumerate(levels):
                        col = param_offset + p_idx * len(levels) + l_idx
                        raw = np.asarray(zarr_array[:n_time, col, :])
                        level_arrays.append(raw)

                    # Stack into (time, level, cell)
                    stacked = np.stack(level_arrays, axis=1)
                    da = xr.DataArray(
                        stacked,
                        dims=["time", "level", "cell"],
                        coords={
                            "time": time_index,
                            "level": ("level", [int(l) for l in levels]),
                        },
                    )
                else:
                    col = param_offset + p_idx
                    raw = np.asarray(zarr_array[:n_time, col, :])
                    da = xr.DataArray(
                        raw,
                        dims=["time", "cell"],
                        coords={"time": time_index},
                    )

                da.attrs["GRIB_paramId"] = param_id
                ds[short_name] = da

            param_offset += len(params) * max(len(levels), 1)

        log_history(ds, "Dataset retrieved by Z3FDB interface")
        return ds

    # ------------------------------------------------------------------
    # Intake DataSource protocol
    # ------------------------------------------------------------------

    def _get_schema(self):
        """Return the intake schema for this source.

        Returns:
            base.Schema
        """
        return base.Schema(
            datashape=None,
            dtype=str(xr.Dataset),
            shape=None,
            name=None,
            npartitions=self._npartitions,
            extra_metadata={},
        )

    def _get_partition(self, i):
        """Retrieve the *i*-th time partition as an ``xr.Dataset``.

        Args:
            i (int): Zero-based partition index.

        Returns:
            xr.Dataset
        """
        chunk_start, chunk_end = self._time_partitions[i]
        self.logger.debug("Reading partition %d: %s → %s", i, chunk_start, chunk_end)
        store = self._build_store(chunk_start, chunk_end)
        return self._zarr_to_xarray(store, chunk_start, chunk_end)

    def read(self):
        """Return the full in-memory dataset concatenated along ``time``.

        Returns:
            xr.Dataset
        """
        self._load_metadata()
        parts = [self._get_partition(i) for i in range(self._npartitions)]
        return xr.concat(parts, dim="time")

    def read_chunked(self):
        """Yield one ``xr.Dataset`` per time partition.

        Yields:
            xr.Dataset
        """
        self._load_metadata()
        for i in range(self._npartitions):
            yield self._get_partition(i)

    def to_dask(self):
        """Return a lazy dask-backed ``xr.Dataset`` spanning all partitions.

        A sample partition is read eagerly to determine variable names, shapes
        and dtypes; subsequent partitions are wrapped in ``dask.delayed`` and
        never read until explicitly computed.

        Returns:
            xr.Dataset
        """
        self._load_metadata()

        # Read the first partition to infer structure
        if self._sample_ds is None:
            self._sample_ds = self._get_partition(0)
        sample = self._sample_ds

        ds_out = xr.Dataset()

        for var in sample.data_vars:
            sample_da = sample[var]
            chunk_arrays = []

            for i, (chunk_start, chunk_end) in enumerate(self._time_partitions):
                n_time = len(self.build_time_coords(chunk_start, chunk_end))
                chunk_shape = (n_time,) + sample_da.shape[1:]

                # Wrap the partition read in a dask.delayed
                delayed_part = dask.delayed(self._get_partition)(i)
                delayed_values = dask.delayed(lambda ds, v: ds[v].values)(delayed_part, var)
                chunk_arrays.append(
                    dask.array.from_delayed(delayed_values, shape=chunk_shape, dtype=sample_da.dtype)
                )

            darr = dask.array.concatenate(chunk_arrays, axis=0)

            # Reconstruct the full time coordinate
            all_times = np.concatenate(
                [self.build_time_coords(s, e).values for s, e in self._time_partitions]
            )
            coords = {k: v for k, v in sample_da.coords.items() if k != "time"}
            coords["time"] = all_times

            ds_out[var] = xr.DataArray(darr, dims=sample_da.dims, coords=coords, attrs=sample_da.attrs)

        ds_out.attrs.update(sample.attrs)
        return ds_out

    # ------------------------------------------------------------------
    # Public helpers (also called from reader_z3fdb in Reader)
    # ------------------------------------------------------------------

    def build_time_coords(self, chunk_start, chunk_end):
        """Public alias for :py:meth:`_build_time_coords`.

        Exposed so that ``Reader.reader_z3fdb`` can call it without reaching
        into a private method.

        Args:
            chunk_start (pd.Timestamp): Chunk start.
            chunk_end (pd.Timestamp): Chunk end.

        Returns:
            pd.DatetimeIndex
        """
        return self._build_time_coords(chunk_start, chunk_end)

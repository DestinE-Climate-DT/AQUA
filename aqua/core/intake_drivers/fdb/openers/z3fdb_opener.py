# Derived from the fdb-xarray library
# https://github.com/koldunovn/fdb-xarray

import copyreg
import os
from collections import defaultdict
from contextlib import closing

import astropy_healpix
import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr
from eccodes.highlevel import StreamReader
from xarray.groupers import TimeResampler

from aqua.core.exceptions import NoDataError
from aqua.core.logger import log_configure, log_history
from aqua.core.util import load_yaml, to_list

from .timeutil import FDBTimeMixin, build_time_request

MARS_KEYS = [
    "date",
    "time",
    "levtype",
    "levelist",
    "step",
    "param",
    "class",
    "stream",
    "domain",
    "type",
    "expver",
    "anoffset",
    "dataset",
    "activity",
    "experiment",
    "generation",
    "model",
    "realization",
    "resolution",
    "frequency",
    "direction",
    "year",
    "month",
]

logger = log_configure(log_level="WARNING", log_name="z3fdb_opener")

# Test if z3fdb module is available
try:
    import pyfdb
    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder
    from z3fdb._internal.zarr import FdbZarrStore

    z3fdb_available = True
except (ImportError, RuntimeError):
    z3fdb_available = False
    z3fdb_error_cause = "z3fdb and pyfdb are required for z3fdb access."

# FdbZarrStore (which accesses FDB data) embeds a C++ wrapped ChunkedDataView that cannot be pickled.
# When running on a Dask distributed cluster, this prevented Dask from serializing
# and sending the tasks to the workers.
# We resolve this by registering a custom pickle reduction handler for FdbZarrStore
# using copyreg.pickle, and capturing the initialization parameters required to reconstruct
# the store on the Dask workers.


def rebuild_fdb_zarr_store(config, mars, serialized_axes, extractor_type_str):
    """Rebuild an FdbZarrStore from serialized parameters. Used for pickling/Dask serialization."""

    fdb_config_file = None
    if config:
        if config.endswith((".yaml", ".yml")):
            fdb_config_file = config
        else:
            os.environ["FDB_HOME"] = config

    builder = SimpleStoreBuilder(fdb_config_file)
    axes = []
    for keys, chunking_spec in serialized_axes:
        if isinstance(chunking_spec, (list, tuple)) and chunking_spec[0] == "FixedSizeChunk":
            chunking = Chunking.FixedSizeChunk(chunkShape=chunking_spec[1])
        else:
            chunking = getattr(Chunking, chunking_spec)
        axes.append(AxisDefinition(keys, chunking))
    extractor_type = getattr(ExtractorType, extractor_type_str)
    if isinstance(mars, list):
        for m in mars:
            builder.add_part(m, axes, extractor_type)
        if len(mars) > 1:
            builder.extend_on_axis(0)
    else:
        builder.add_part(mars, axes, extractor_type)
    store = builder.build()

    # Attach serialization attributes to the rebuilt store as well
    store._config = config
    store._mars = mars
    store._serialized_axes = serialized_axes
    store._extractor_type_str = extractor_type_str

    return store


def reduce_fdb_zarr_store(store):
    """Reduce FdbZarrStore to a serializable state for pickling/Dask serialization."""
    has_attrs = (
        hasattr(store, "_config")
        and hasattr(store, "_mars")
        and hasattr(store, "_serialized_axes")
        and hasattr(store, "_extractor_type_str")
    )
    if not has_attrs:
        raise TypeError("Cannot pickle FdbZarrStore: missing serialization attributes")
    return (rebuild_fdb_zarr_store, (store._config, store._mars, store._serialized_axes, store._extractor_type_str))


# Register custom pickle reducer if z3fdb is available
if z3fdb_available:
    copyreg.pickle(FdbZarrStore, reduce_fdb_zarr_store)


def _check_availability():
    """Check if z3fdb is available."""
    if not z3fdb_available:
        raise ImportError(z3fdb_error_cause)


def _validate_time_settings(freq, timestyle, timestep):
    """Normalize GSV-style time settings and reject unsupported calendar units."""
    freq = freq or timestep
    freq = {"M": "MS", "Y": "YS"}.get(freq, freq)
    try:
        cadence = pd.tseries.frequencies.to_offset(freq)
        base = pd.tseries.frequencies.to_offset(timestep or freq)
    except ValueError as exc:
        raise ValueError("Invalid z3fdb savefreq or timestep.") from exc
    calendar = cadence.n == 1 and (
        isinstance(cadence, pd.offsets.MonthBegin) or isinstance(cadence, pd.offsets.YearBegin) and cadence.month == 1
    )
    if not calendar and not isinstance(cadence, (pd.offsets.Tick, pd.offsets.Day)):
        raise ValueError("z3fdb savefreq requires fixed minute/hour/day units, MS or YS.")
    if cadence.n <= 0 or not isinstance(base, (pd.offsets.Tick, pd.offsets.Day)) or base.nanos < pd.Timedelta(minutes=1).value:
        raise ValueError("z3fdb timestep requires positive fixed units of at least one minute.")
    if base.nanos % pd.Timedelta(minutes=1).value:
        raise ValueError("z3fdb timestep requires whole minutes.")
    if isinstance(cadence, (pd.offsets.Tick, pd.offsets.Day)) and (cadence.nanos < base.nanos or cadence.nanos % base.nanos):
        raise ValueError("z3fdb savefreq must be an integer multiple of timestep.")
    freq = "YS" if isinstance(cadence, pd.offsets.YearBegin) else cadence.freqstr
    if timestyle is None:
        timestyle = "yearmonth" if freq == "MS" else "date"
    if timestyle not in ("date", "step", "yearmonth"):
        raise ValueError(f"Unsupported z3fdb timestyle {timestyle!r}; use date, step or yearmonth.")
    if timestyle == "yearmonth" and not calendar:
        raise ValueError("z3fdb timestyle='yearmonth' requires monthly/yearly savefreq.")
    return freq, timestyle, base


def _build_mars_requests(
    request, freq, levels, years, start_date=None, end_date=None, timestyle=None, timestep="h", data_start_date=None
):
    """Build requests and exact output times using the shared FDB sampling rules."""
    freq, timestyle, base = _validate_time_settings(freq, timestyle, timestep)
    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError("provide both start_date and end_date")
    else:
        years = to_list(years if years is not None else request["year"])
        start_date = f"{min(map(int, years))}-01-01"
        end_date = f"{max(map(int, years))}-12-31T23:00"
    start = pd.Timestamp(FDBTimeMixin._floor_datetime(start_date, freq))
    end = pd.Timestamp(FDBTimeMixin._floor_datetime(end_date, freq))
    if start > end:
        raise ValueError("z3fdb start_date must not be later than end_date.")
    if timestyle == "step" and data_start_date is None:
        raise ValueError("z3fdb timestyle='step' requires data_start_date as the forecast reference time.")
    reference = pd.Timestamp(FDBTimeMixin._floor_datetime(data_start_date or start_date, freq))
    clock = pd.Timedelta(0)
    fixed_clock = timestyle == "date" and (
        not isinstance(pd.tseries.frequencies.to_offset(freq), (pd.offsets.Tick, pd.offsets.Day))
        or pd.tseries.frequencies.to_offset(freq).nanos >= pd.Timedelta(days=1).value
    )
    if fixed_clock:
        value = request.get("time")
        if value is None or value == "None":
            value = "0000"
        values = to_list(value)
        if len(values) != 1 or not str(values[0]).isdigit() or len(str(values[0])) > 4:
            raise ValueError("Daily/monthly z3fdb date requests require one numeric time (HH or HHMM).")
        text = str(values[0])
        hour, minute = (int(text), 0) if len(text) <= 2 else divmod(int(text), 100)
        if hour > 23 or minute > 59:
            raise ValueError("Invalid z3fdb request time; use HH or HHMM.")
        clock = pd.Timedelta(hours=hour, minutes=minute)
    start += clock
    end += clock
    if timestyle != "yearmonth":
        step = to_list(request.get("step", 0))
        if len(step) != 1 or not str(step[0]).isdigit():
            raise ValueError("z3fdb requires one non-negative initial forecast step.")
        if int(step[0]):
            start = max(start, pd.Timestamp(str(data_start_date or start_date)) + int(step[0]) * base)
    if start > end:
        raise NoDataError("No timestamps remain after applying the initial forecast step.")
    dates = pd.date_range(start, end, freq=base)
    offset = clock if isinstance(pd.tseries.frequencies.to_offset(freq), (pd.offsets.Tick, pd.offsets.Day)) else None
    indices = FDBTimeMixin._saved_time_indices(dates, freq, offset=offset)
    time = dates[indices]
    if fixed_clock:
        clock_time = time.normalize() + clock
        time = time.where(time >= clock_time, clock_time)
    if not len(time):
        raise NoDataError("No timestamps selected by the z3fdb time settings.")

    if timestyle == "step":
        delta = time - reference
        if (delta % pd.Timedelta(base) != pd.Timedelta(0)).any():
            raise ValueError("Requested forecast timestamps are not aligned with timestep and data_start_date.")
        steps = np.asarray(delta / pd.Timedelta(base), dtype=int)
        part = build_time_request(
            timestyle,
            time[0],
            time[-1],
            reference=(reference.strftime("%Y%m%d"), reference.strftime("%H%M")),
            steps=(steps[0], steps[-1]),
        )
        if len(steps) > 1:
            if (np.diff(steps) == 1).all():
                part["step"] += "/by/1"
            else:
                part["step"] = "/".join(map(str, steps))
        parts = [part]
    elif timestyle == "date":
        groups = []
        for day, values in pd.Series(time).groupby(time.normalize(), sort=False):
            pattern = tuple(values.dt.strftime("%H%M"))
            if groups and groups[-1][0] == pattern:
                groups[-1][1].append(day)
                groups[-1][3] = values.iloc[-1]
            else:
                groups.append([pattern, [day], values.iloc[0], values.iloc[-1]])
        parts = []
        for pattern, days, first, last in groups:
            part = build_time_request(timestyle, first, last)
            if (days[-1] - days[0]).days + 1 != len(days):
                part["date"] = "/".join(day.strftime("%Y%m%d") for day in days)
            if len(pattern) > 1:
                if (np.diff(pd.to_datetime(list(pattern), format="%H%M")) == pd.Timedelta(hours=1)).all():
                    part["time"] += "/by/1"
                else:
                    part["time"] = "/".join(pattern)
            parts.append(part)
    else:
        year_months = defaultdict(list)
        for date in time:
            year_months[date.year].append(date.month)
        groups = []
        for year, months in year_months.items():
            months = tuple(months)
            if groups and groups[-1][1] == months:
                groups[-1][0].append(year)
            else:
                groups.append(([year], months))
        parts = []
        for years_list, months in groups:
            part = build_time_request(
                timestyle, pd.Timestamp(years_list[0], months[0], 1), pd.Timestamp(years_list[-1], months[-1], 1)
            )
            part["year"] = "/".join(map(str, years_list))
            part["month"] = "/".join(map(str, months))
            parts.append(part)

    mars_list = []
    for part in parts:
        req = request.copy()
        for key in ("date", "time") if timestyle == "yearmonth" else ("year", "month"):
            req.pop(key, None)
        req.update(part)
        mars_list.append(req)
    return mars_list, time


def _build_zarr_axes(freq, levels, chunks=None, timestyle=None, time=None):
    """Build the AxisDefinition objects representing the layout of the virtual Zarr store."""
    _, timestyle, _ = _validate_time_settings(freq, timestyle, "1min")
    time_keys = {"date": ["date", "time"], "yearmonth": ["year", "month"], "step": ["step"]}[timestyle]

    chunking = Chunking.SINGLE_VALUE  # by default we chunk step-by-step in time
    val = chunks.get("time") if isinstance(chunks, dict) else chunks
    chunk_error = (
        "z3fdb time chunks must be 'S', a positive frequency (e.g. 'D', 'MS', '6h') or a positive integer sample count."
    )
    if isinstance(val, str):
        if val.isdigit():
            val = int(val)
        elif val.upper() in ("S", "SINGLE"):
            val = 1
        else:
            if time is None:
                raise ValueError("Frequency-based time chunks require the planned timestamps.")
            try:
                offset = pd.tseries.frequencies.to_offset(val)
                if offset.n <= 0:
                    raise ValueError("Chunk frequency must be positive.")
                # Native chunks must divide every full or partial time bin in this store.
                sizes = pd.Series(0, index=time).resample(offset).size().to_numpy()
            except ValueError as exc:
                raise ValueError(chunk_error) from exc
            val = int(np.gcd.reduce(sizes))
    if val is None:
        val = 1
    if not isinstance(val, int) or val < 1:
        raise ValueError(chunk_error)
    if val > 1:
        chunking = Chunking.FixedSizeChunk(chunkShape=val)
    time_axes = [AxisDefinition(time_keys, chunking)]

    level_axes = []
    if levels is not None:
        chunking = Chunking.WHOLE_AXIS
        if isinstance(chunks, dict):
            val = chunks.get("vertical") or chunks.get("level")
            if isinstance(val, str) and val.isdigit():
                val = int(val)

            if isinstance(val, int) and val > 0:
                n_levels = len(to_list(levels))
                if val == 1:
                    chunking = Chunking.SINGLE_VALUE
                elif val == n_levels:
                    chunking = Chunking.WHOLE_AXIS
                elif n_levels % val == 0:
                    chunking = Chunking.FixedSizeChunk(chunkShape=val)
                else:
                    logger.warning(
                        "Vertical chunk size %s is not an integer divisor of number of levels (%s); "
                        "falling back to SINGLE_VALUE.",
                        val,
                        n_levels,
                    )
                    chunking = Chunking.SINGLE_VALUE

        level_axes = [AxisDefinition(["levelist"], chunking)]

    axes = time_axes + [AxisDefinition(["param"], Chunking.SINGLE_VALUE)] + level_axes
    return axes


def _chunk_by_time(data, chunks):
    """Combine native chunks lazily into the requested time-frequency bins."""
    val = chunks.get("time") if isinstance(chunks, dict) else chunks
    if isinstance(val, str) and not val.isdigit() and val.upper() not in ("S", "SINGLE"):
        return data.chunk(time=TimeResampler(val))
    return data


def to_dataset(
    zarr_array,
    keys,
    start_date,
    freq="1h",
    variables=None,
    levels=None,
    time=None,
):
    """Wrap a z3fdb zarr array as an xarray.Dataset with one variable per param.

    Zarr shape is (time, param, cell) or, when levels is given,
    (time, param, level, cell). Each param slice becomes its own
    DataArray; dask keeps everything lazy.

    Args:
        zarr_array (zarr.Array): Zarr array containing the data.
        keys (list): List of variable names.
        start_date (str): Start date of the data.
        freq (str, optional): Frequency of the data. Defaults to "1h".
        variables (list, optional): List of variable names. Defaults to None.
        levels (list, optional): List of levels. Defaults to None.
        time (pd.DatetimeIndex, optional): Exact requested timestamps. Defaults to start_date/freq for compatibility.

    Returns:
        xarray.Dataset: Lazy (dask-backed) xr.Dataset.
    """

    has_level = levels is not None
    if has_level:
        nt, nparam, nlev, ncell = zarr_array.shape
        if len(levels) != nlev:
            raise ValueError(f"levels has {len(levels)} entries, level axis has {nlev}")
    else:
        nt, nparam, ncell = zarr_array.shape

    if len(keys) != nparam:
        raise ValueError(f"keys has {len(keys)} entries, param axis has {nparam}")

    darr = da.from_zarr(zarr_array)
    if time is None:
        time = pd.date_range(start=str(start_date), periods=nt, freq=freq)
    if len(time) != nt:
        raise ValueError(f"Requested time axis has {len(time)} timestamps, but the z3fdb array has {nt} time entries.")
    cell = np.arange(ncell, dtype=np.int64)

    data_vars = {}
    for i, name in enumerate(keys):
        if has_level:
            arr = darr[:, i, :, :]
            dims = ("time", "level", "cell")
            coords = {
                "time": time,
                "level": list(levels),
                "cell": cell,
            }
        else:
            arr = darr[:, i, :]
            dims = ("time", "cell")
            coords = {
                "time": time,
                "cell": cell,
            }

        if isinstance(name, int) or name.isdigit():
            name = "var" + str(name)
        data_vars[name] = xr.DataArray(
            arr,
            dims=dims,
            coords=coords,
            name=name,
        )

    ds = xr.Dataset(data_vars)
    return ds


def _read_grib_grid(mars, config=None):
    """Decode the grid of one field using the same FDB configuration as z3fdb."""
    sample = {key: str(to_list(value)[0]).split("/")[0] for key, value in mars.items()}
    fdb = pyfdb.FDB(config=load_yaml(config) if config else None)
    with closing(fdb.retrieve(sample)) as stream:
        message = next(StreamReader(stream), None)
    if message is None:
        raise NoDataError("No GRIB message found to identify the z3fdb grid.")
    grid_type = message["gridType"]
    keys = {
        "healpix": ("Nside", "ordering"),
        "regular_ll": ("Ni", "Nj", "latitudeOfFirstGridPoint"),
    }
    if grid_type not in keys:
        raise ValueError(f"Unsupported z3fdb grid type: {grid_type}; only HEALPix and global regular lon/lat are supported.")
    return {"grid_type": grid_type, **{key: message[key] for key in keys[grid_type]}}


def add_coordinates(ds, grid, levunits=None):
    """Attach coordinates from a decoded HEALPix or global regular lon/lat grid.

    Args:
        ds (xr.Dataset): Dataset with a flat cell dimension in GRIB value order.
        grid (dict): Grid type and defining keys decoded from a GRIB message.
        levunits (str, optional): Vertical coordinate units. Defaults to None.

    Returns:
        xr.Dataset: Dataset with coordinates and lazy field data.

    Raises:
        ValueError: When the grid is unsupported or its size does not match the data.
    """
    if levunits:
        ds.level.attrs["units"] = levunits
    grid_type = grid["grid_type"]
    ncell = ds.sizes["cell"]
    if grid_type == "healpix":
        nside, ordering = grid["Nside"], grid["ordering"]
        if nside <= 0 or nside & (nside - 1) or ordering not in (0, 1) or ncell != 12 * nside**2:
            raise ValueError("GRIB HEALPix resolution, ordering or point count does not match the z3fdb cell dimension.")
        hp = astropy_healpix.HEALPix(nside=nside, order="nested" if ordering else "ring")
        lon, lat = hp.healpix_to_lonlat(np.arange(ncell))
        lon_bounds, lat_bounds = hp.boundaries_lonlat(np.arange(ncell), step=1)
        coordinates = {}
        for name, values, bounds, standard_name, units in [
            ("lon", lon, lon_bounds, "longitude", "degrees_east"),
            ("lat", lat, lat_bounds, "latitude", "degrees_north"),
        ]:
            coordinates[name] = xr.Variable(
                ("cell",),
                values.to_value("deg"),
                attrs={"standard_name": standard_name, "units": units, "bounds": f"{name}_bounds"},
            )
            coordinates[f"{name}_bounds"] = xr.Variable(
                ("cell", "cell_corners"), bounds.to_value("deg"), attrs={"units": units}
            )
        ds = ds.assign_coords(coordinates).rename_dims(cell="ncells").drop_vars("cell", errors="ignore")
    elif grid_type == "regular_ll":
        ni, nj = grid["Ni"], grid["Nj"]
        if ni <= 0 or nj <= 0 or ncell != ni * nj:
            raise ValueError("GRIB grid point count does not match the z3fdb cell dimension.")
        variables = {}
        for name, var in ds.data_vars.items():
            values = var.data.reshape(var.shape[:-1] + (nj, ni))
            if grid["latitudeOfFirstGridPoint"] < 0:
                values = values[..., ::-1, :]
            variables[name] = xr.Variable(var.dims[:-1] + ("lat", "lon"), values, attrs=var.attrs)
        ds = (
            ds.drop_dims("cell")
            .assign(variables)
            .assign_coords(
                lon=("lon", np.linspace(0, 360, ni, endpoint=False), {"standard_name": "longitude", "units": "degrees_east"}),
                lat=("lat", np.linspace(90, -90, nj), {"standard_name": "latitude", "units": "degrees_north"}),
            )
        )
    else:
        raise ValueError(f"Unsupported z3fdb grid type: {grid_type}; only HEALPix and global regular lon/lat are supported.")
    log_history(ds, f"Spatial coordinates decoded from GRIB grid {grid}")
    return ds


def open_z3fdb(
    request,
    variables=None,
    levels=None,
    config=None,
    years=None,
    startdate=None,
    enddate=None,
    data_start_date=None,
    data_end_date=None,
    freq="MS",
    chunks=None,
    level_values=None,
    grid=None,
    timestyle=None,
    timestep="h",
    timeshift=None,
):
    """Open a Climate DT FDB selection as an xarray.Dataset using z3fdb.

    Constructs axis definitions and time coordinate accordingly.
    Returns a lazy (dask-backed) xr.Dataset.

    Args:
        request (dict): Dictionary containing the FDB request. Keys outside MARS_KEYS are ignored.
        variables (list, optional): List of variables to request. Defaults to None.
        levels (int or list, optional): Level IDs to request. Defaults to the request levelist.
        config (str, optional): Path to the configuration file. Defaults to "./config.yaml".
        years (range, optional): Range of years to request. Defaults to None.
        startdate (str, optional): Start date of the data. Defaults to None.
        enddate (str, optional): End date of the data. Defaults to None.
        data_start_date (str, optional): Start date of the dataset. Defaults to None.
        data_end_date (str, optional): End date of the dataset. Defaults to None.
        freq (str, optional): Frequency of the data. Defaults to "MS".
        chunks (str, int or dict, optional): Chunking configuration for the zarr array.
            Time accepts 'S' for single frames, frequencies such as 'D', 'MS', '6h',
            or an integer frame count. A string/integer sets time chunking; dictionaries
            use 'time' and 'level'/'vertical'. Fixed sizes must divide their axes exactly.
            Frequencies support partial time bins and any saved cadence. Defaults to None.
        level_values (float or list, optional): Physical values matching the full request levelist.
            Defaults to the level IDs.
        grid (str, optional): Legacy hint, ignored. The GRIB message defines the grid.
        timestyle (str, optional): Explicit date, step or yearmonth storage layout. Defaults to inference from freq.
        timestep (str, optional): Native step units, e.g. "h" or "15min". Defaults to "h".
        timeshift (bool, optional): Legacy monthly shifting is unsupported. Defaults to None.

    Returns:
        xarray.Dataset: Lazy (dask-backed) xr.Dataset.
    """

    if timeshift:
        raise ValueError("Legacy timeshift is not supported by z3fdb.")
    freq, timestyle, _ = _validate_time_settings(freq, timestyle, timestep)
    _check_availability()

    request = {key: value for key, value in request.items() if key in MARS_KEYS}

    if variables:
        request["param"] = variables
    else:
        variables = request.get("param", None)

    if not isinstance(variables, (list, tuple, range)):
        variables = [variables]

    levelist = to_list(request.get("levelist")) or None
    levels = to_list(levels) or levelist
    if levels is not None:
        request["levelist"] = levels

    level_values = to_list(level_values) or levelist or levels

    if level_values and levelist:
        level_values = [level_values[levelist.index(l)] for l in levels]

    # Set level units based on level type
    levtype = request.get("levtype", None)
    levtype = levtype.lower() if levtype else None
    if levtype == "pl":
        levunits = "hPa"
    elif levtype == "o3d" or levtype == "hl":
        levunits = "m"
    else:
        levunits = None

    # if years is not defined and startdate and enddate are not defined
    # then we use data_start_date to define startdate

    if years is None:
        if startdate is None:
            startdate = data_start_date
        if enddate is None:
            enddate = data_end_date

    mars_list, time = _build_mars_requests(
        request,
        freq,
        levels,
        years,
        startdate,
        enddate,
        timestyle=timestyle,
        timestep=timestep,
        data_start_date=data_start_date,
    )
    axes = _build_zarr_axes(freq, levels, chunks, timestyle=timestyle, time=time)

    # Create zarr store
    fdb_config_file = None
    if config:
        if config.endswith((".yaml", ".yml")):
            fdb_config_file = config
        else:
            os.environ["FDB_HOME"] = config

    builder = SimpleStoreBuilder(fdb_config_file)
    for mars in mars_list:
        builder.add_part(mars, axes, ExtractorType.GRIB)
    if len(mars_list) > 1:
        builder.extend_on_axis(0)
    store = builder.build()

    # Attach serialization attributes for pickling/Dask support
    store._config = config
    store._mars = mars_list
    serialized_axes = []
    for axis in axes:
        if hasattr(axis.chunking, "chunkShape"):
            serialized_axes.append((axis.keys, ("FixedSizeChunk", axis.chunking.chunkShape)))
        else:
            serialized_axes.append((axis.keys, axis.chunking.name))
    store._serialized_axes = serialized_axes
    store._extractor_type_str = ExtractorType.GRIB.name

    zarr_arr = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    ds = to_dataset(
        zarr_arr,
        keys=variables,
        start_date=time[0],
        levels=level_values,
        time=time,
    )

    source_grid = _read_grib_grid(mars_list[0], fdb_config_file)
    ds = add_coordinates(ds, source_grid, levunits=levunits)

    # Convert list of dictionaries to list of strings
    def _mars_to_str(req):
        return ",".join(f"{k}=" + ("/".join(map(str, v)) if isinstance(v, (list, tuple)) else str(v)) for k, v in req.items())

    mars_str_list = [_mars_to_str(m) for m in mars_list]

    ds.attrs.update(
        {
            "mars_request": "; ".join(mars_str_list),
        }
    )

    return _chunk_by_time(ds, chunks)

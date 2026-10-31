# Derived from the fdb-xarray library
# https://github.com/koldunovn/fdb-xarray

import copy
import copyreg
import os
from collections import defaultdict
from math import prod

import astropy_healpix
import dask.array as da
import intake
import numpy as np
import pandas as pd
import xarray as xr
import zarr

from aqua.core.data_model import CoordIdentifier
from aqua.core.default import AQUA_LATITUDE, AQUA_LONGITUDE
from aqua.core.logger import log_configure, log_history
from aqua.core.regridder.grid_dict_handler import GridDictHandler
from aqua.core.regridder.regridder_util import get_grid_path
from aqua.core.util import to_list

xr.set_options(keep_attrs=True)

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
    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder
    from z3fdb._internal.zarr import FdbZarrStore

    z3fdb_available = True
except ImportError:
    z3fdb_available = False
    z3fdb_error_cause = "z3fdb cannot be imported."

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


def _build_mars_requests(request, freq, levels, years, start_date=None, end_date=None):
    """Build the list of MARS request strings based on start/end dates and frequency."""
    if years is None and (start_date is None or end_date is None):
        years = request["year"]
        if not isinstance(years, (list, tuple, range)):
            years = [years]
        years = [str(y) for y in years]

    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError("provide both start_date and end_date")
        ts_start = pd.Timestamp(str(start_date))
        ts_end = pd.Timestamp(str(end_date))
    else:
        ys = sorted(list(int(y) for y in years))
        ts_start = pd.Timestamp(f"{ys[0]}-01-01T00:00:00")
        ts_end = pd.Timestamp(f"{ys[-1]}-12-31T23:00:00")

    parts = []

    if freq == "h":
        if ts_start == ts_end:
            parts.append({"date": ts_start.strftime("%Y%m%d"), "time": f"{ts_start.hour:02d}00"})
        else:
            day_start = ts_start.floor("D")
            day_end = ts_end.floor("D")

            if day_start == day_end:
                parts.append(
                    {
                        "date": ts_start.strftime("%Y%m%d"),
                        "time": (
                            f"{ts_start.hour:02d}00/to/{ts_end.hour:02d}00/by/1"
                            if ts_start.hour != ts_end.hour
                            else f"{ts_start.hour:02d}00"
                        ),
                    }
                )
            else:
                # First day partial part
                if ts_start.hour > 0:
                    parts.append({"date": ts_start.strftime("%Y%m%d"), "time": f"{ts_start.hour:02d}00/to/2300/by/1"})
                    day_start_full = day_start + pd.Timedelta(days=1)
                else:
                    day_start_full = day_start

                # Last day partial part
                if ts_end.hour < 23:
                    day_end_full = day_end - pd.Timedelta(days=1)
                    end_part = {"date": ts_end.strftime("%Y%m%d"), "time": f"0000/to/{ts_end.hour:02d}00/by/1"}
                else:
                    day_end_full = day_end
                    end_part = None

                # Mid-range full days
                if day_start_full <= day_end_full:
                    if day_start_full == day_end_full:
                        date_val = day_start_full.strftime("%Y%m%d")
                    else:
                        date_val = f"{day_start_full.strftime('%Y%m%d')}/to/{day_end_full.strftime('%Y%m%d')}/by/1"
                    parts.append({"date": date_val, "time": "0000/to/2300/by/1"})

                if end_part is not None:
                    parts.append(end_part)

        pd_freq = "1h"
        start = ts_start.isoformat()

    elif freq == "D":
        if ts_start.date() == ts_end.date():
            date_val = ts_start.strftime("%Y%m%d")
        else:
            date_val = f"{ts_start.strftime('%Y%m%d')}/to/{ts_end.strftime('%Y%m%d')}/by/1"

        parts.append({"date": date_val, "time": request.get("time", "0000")})
        pd_freq = "1D"
        start = ts_start.strftime("%Y-%m-%d")

    elif freq == "MS":
        months_range = pd.date_range(start=ts_start, end=ts_end, freq="MS")

        year_months = defaultdict(list)
        for dt in months_range:
            year_months[dt.year].append(dt.month)

        groups = []
        current_months = None
        current_years = []

        for year in sorted(year_months.keys()):
            months = tuple(sorted(year_months[year]))
            if current_months is None:
                current_months = months
                current_years = [year]
            elif months == current_months:
                current_years.append(year)
            else:
                groups.append((current_years, current_months))
                current_months = months
                current_years = [year]
        if current_years:
            groups.append((current_years, current_months))

        for years_list, months_tuple in groups:
            parts.append(
                {
                    "year": "/".join(str(y) for y in years_list) if len(years_list) > 1 else str(years_list[0]),
                    "month": "/".join(str(m) for m in months_tuple),
                }
            )

        pd_freq = "MS"
        start = ts_start.strftime("%Y-%m-%d")
    else:
        raise ValueError(f"Unknown freq {freq!r}")

    # Create list of mars requests
    mars_list = []
    for part in parts:
        req_copy = copy.deepcopy(request)
        req_copy.update(part)
        if freq in ("h", "D"):
            req_copy.pop("year", None)
            req_copy.pop("month", None)
        elif freq == "MS":
            req_copy.pop("date", None)
            req_copy.pop("time", None)

        mars_list.append(req_copy)

    return mars_list, pd_freq, start


def _build_zarr_axes(freq, levels, chunks=None):
    """Build the AxisDefinition objects representing the layout of the virtual Zarr store."""
    if freq in ("h", "D"):
        time_keys = ["date", "time"]
    elif freq == "MS":
        time_keys = ["year", "month"]
    else:
        raise ValueError(f"Unknown freq {freq!r}")

    chunking = Chunking.SINGLE_VALUE  # by default we chunk step-by-step in time
    val = chunks.get("time") if isinstance(chunks, dict) else chunks
    if isinstance(val, str) and val.isdigit():
        val = int(val)
    if isinstance(val, int) and val > 1:
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


def to_dataset(
    zarr_array,
    keys,
    start_date,
    freq="1h",
    variables=None,
    levels=None,
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
    time = pd.date_range(start=str(start_date), periods=nt, freq=freq)
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


def add_healpix_coordinates(ds, nside=None, ordering=None):
    """Add coordinates for an explicitly declared full HEALPix grid.

    Args:
        ds (xr.Dataset): Dataset with a flat cell dimension.
        nside (int, optional): Declared HEALPix resolution. Required.
        ordering (str, optional): Declared 'nested' or 'ring' ordering. Required.

    Returns:
        xr.Dataset: Dataset with coordinates and bounds in the stored cell order.

    Raises:
        ValueError: When the grid declaration is incomplete or has the wrong size.
    """
    if not isinstance(nside, int) or nside <= 0 or nside & (nside - 1) or ordering not in ("nested", "ring"):
        raise ValueError("HEALPix coordinates require a positive power-of-two nside and explicit nested/ring ordering.")
    ncell = ds.sizes["cell"]
    if ncell != 12 * nside**2:
        raise ValueError("Data cell count does not match the declared HEALPix resolution.")

    hp = astropy_healpix.HEALPix(nside=nside, order=ordering)
    lon, lat = hp.healpix_to_lonlat(np.arange(ncell))
    lon_vals = lon.to_value("deg")
    lat_vals = lat.to_value("deg")

    lon_b, lat_b = hp.boundaries_lonlat(np.arange(ncell), step=1)
    lon_b_vals = lon_b.to_value("deg")
    lat_b_vals = lat_b.to_value("deg")

    ds = ds.assign_coords(
        {
            "lon": (
                ("cell",),
                lon_vals,
                {"units": "degrees_east", "standard_name": "longitude", "long_name": "longitude", "bounds": "lon_bounds"},
            ),
            "lat": (
                ("cell",),
                lat_vals,
                {"units": "degrees_north", "standard_name": "latitude", "long_name": "latitude", "bounds": "lat_bounds"},
            ),
            "lon_bounds": (("cell", "vertices"), lon_b_vals, {"units": "degrees_east"}),
            "lat_bounds": (("cell", "vertices"), lat_b_vals, {"units": "degrees_north"}),
        }
    )
    return ds


def _add_grid_file_coordinates(ds, grid, cache):
    """Attach file coordinates in the declared order of the flat FDB values."""
    if "grid_data" not in cache:
        cache["grid_data"] = intake.open_netcdf(get_grid_path(grid["path"])).to_dask()
    grid_data = cache["grid_data"]
    try:
        if "identified" not in cache:
            bounds = {var.attrs["bounds"] for var in grid_data.variables.values() if var.attrs.get("bounds")}
            candidates = grid_data.drop_vars(bounds | {"lon_bounds", "lat_bounds", "lon_bnds", "lat_bnds"}, errors="ignore")
            cache["identified"] = CoordIdentifier(
                candidates.set_coords(list(candidates.data_vars)).coords, loglevel=logger.getEffectiveLevel()
            ).identify_coords()
        identified = cache["identified"]
        longitude, latitude = identified[AQUA_LONGITUDE], identified[AQUA_LATITUDE]
        if longitude is None or latitude is None:
            raise ValueError("The z3fdb grid file must identify longitude and latitude coordinates.")
        names = [longitude["name"], latitude["name"]]
        horizontal_dims = set(longitude["dims"]) | set(latitude["dims"])
        dims = to_list(grid.get("fdb_dims"))
        if not dims and len(horizontal_dims) == 1:
            dims = list(horizontal_dims)
        if not dims or set(dims) != horizontal_dims or len(dims) != len(horizontal_dims):
            raise ValueError("Structured z3fdb grids require fdb_dims listing their storage dimensions, slowest first.")
        if horizontal_dims & {"time", "level"}:
            raise ValueError("Grid longitude/latitude must depend only on horizontal dimensions.")
        if prod(grid_data.sizes[dim] for dim in dims) != ds.sizes["cell"]:
            raise ValueError("Data cell count does not match the grid coordinate file.")

        for name in list(names):
            bounds = grid_data[name].attrs.get("bounds")
            if bounds:
                if bounds not in grid_data:
                    raise ValueError(f"Grid coordinate {name} refers to missing bounds {bounds}.")
                names.append(bounds)
        names.extend(name for name in ("lon_bounds", "lat_bounds", "lon_bnds", "lat_bnds") if name in grid_data)

        if len(dims) == 1:
            rename = {dims[0]: "cell"}
        else:
            if "cell" in horizontal_dims:
                raise ValueError("Structured grid dimensions must use names other than cell.")
            shape = tuple(grid_data.sizes[dim] for dim in dims)
            variables = {
                name: xr.Variable(var.dims[:-1] + tuple(dims), var.data.reshape(var.shape[:-1] + shape), attrs=var.attrs)
                for name, var in ds.data_vars.items()
            }
            ds = ds.drop_dims("cell").assign(variables)
            rename = {}
        if any(set(grid_data[name].dims) & {"time", "level"} for name in names):
            raise ValueError("Grid coordinates and bounds must not depend on time or level.")
        coordinates = {
            name: xr.Variable(
                tuple(rename.get(dim, dim) for dim in grid_data[name].dims), grid_data[name].data, attrs=grid_data[name].attrs
            )
            for name in names
        }
        ds = ds.assign_coords(coordinates)
    except Exception:
        grid_data.close()
        raise
    ds.set_close(grid_data.close)
    return ds


def add_coordinates(ds, levunits=None, grid_type=None, grid=None, _cache=None):
    """Attach coordinates from an explicit HEALPix declaration or grid file.

    Args:
        ds (xr.Dataset): Dataset with a flat cell dimension.
        levunits (str, optional): Units of the level coordinate. Defaults to None.
        grid_type (str, optional): Legacy grid argument; ambiguous types are rejected.
        grid (str or dict, optional): Explicit grid name or resolved definition. Defaults to None.
        _cache (dict, optional): Coordinate cache shared within one Intake read. Defaults to None.

    Returns:
        xr.Dataset: Dataset with validated spatial coordinates and unchanged field ordering.

    Raises:
        ValueError: When the grid is ambiguous or its shape does not match the data.
    """
    if levunits:
        ds.level.attrs["units"] = levunits
    grid = GridDictHandler({"grids": {}}).normalize_fdb_grid(grid if grid is not None else grid_type)
    cache = {} if _cache is None else _cache
    if "nside" in grid:
        if "coordinates" not in cache:
            ds = add_healpix_coordinates(ds, nside=grid["nside"], ordering=grid.get("ordering"))
            cache["coordinates"] = {name: ds.coords[name] for name in ("lon", "lat", "lon_bounds", "lat_bounds")}
        elif ds.sizes["cell"] != cache["coordinates"]["lon"].size:
            raise ValueError("Data cell count does not match the declared HEALPix resolution.")
        else:
            ds = ds.assign_coords(cache["coordinates"])
    else:
        ds = _add_grid_file_coordinates(ds, grid, cache)
    log_history(ds, f"Spatial coordinates assigned from declared z3fdb grid {grid}")
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
    _coordinate_cache=None,
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
        chunks (dict, optional): Chunking configuration for the zarr array.
            Supports 'level' or 'vertical' keys. If the value is an integer > 1,
            Chunking.FixedSizeChunk is used with that chunk size.
            If the value is 1 (or 'single', True), Chunking.SINGLE_VALUE is used.
            The time axis is always chunked as single values. Defaults to None.
        level_values (float or list, optional): Physical values matching the full request levelist.
            Defaults to the level IDs.
        grid (str or dict, optional): Explicit HEALPix name or resolved grid-file definition.
            Required for geographic coordinates; defaults to None.
        _coordinate_cache (dict, optional): Coordinate cache for one Intake read. Defaults to None.

    Returns:
        xarray.Dataset: Lazy (dask-backed) xr.Dataset.
    """

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

    mars_list, pd_freq, start = _build_mars_requests(request, freq, levels, years, startdate, enddate)
    axes = _build_zarr_axes(freq, levels, chunks)

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
        start_date=start,
        freq=pd_freq,
        levels=level_values,
    )

    ds = add_coordinates(ds, levunits=levunits, grid=grid, _cache=_coordinate_cache)

    # Convert list of dictionaries to list of strings
    def _mars_to_str(req):
        return ",".join(f"{k}=" + ("/".join(map(str, v)) if isinstance(v, (list, tuple)) else str(v)) for k, v in req.items())

    mars_str_list = [_mars_to_str(m) for m in mars_list]

    ds.attrs.update(
        {
            "mars_request": "; ".join(mars_str_list),
        }
    )

    return ds

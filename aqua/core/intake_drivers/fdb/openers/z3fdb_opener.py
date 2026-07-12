# Derived from the fdb-xarray library
# https://github.com/koldunovn/fdb-xarray

import copy
import copyreg
import os

import astropy_healpix
import dask.array as da
import numpy as np
import pandas as pd
import xarray as xr
import zarr

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
    from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

    fdb_config_file = None
    if config:
        if config.endswith((".yaml", ".yml")):
            fdb_config_file = config
        else:
            os.environ["FDB_HOME"] = config

    builder = SimpleStoreBuilder(fdb_config_file)
    axes = []
    for keys, chunking_str in serialized_axes:
        chunking = getattr(Chunking, chunking_str)
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


def _mars_date(s):  # "2014-01-15" or "20140115" -> "20140115"
    return str(s).replace("-", "")[:8]


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

        from collections import defaultdict

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

        m_str = ",".join(f"{k}=" + ("/".join(map(str, v)) if isinstance(v, list) else str(v)) for k, v in req_copy.items())
        mars_list.append(m_str)

    return mars_list, pd_freq, start


def _build_zarr_axes(freq, levels, chunks=None):
    """Build the AxisDefinition objects representing the layout of the virtual Zarr store."""
    if freq in ("h", "D"):
        time_axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
    elif freq == "MS":
        time_axes = [AxisDefinition(["year", "month"], Chunking.SINGLE_VALUE)]
    else:
        raise ValueError(f"Unknown freq {freq!r}")

    level_axes = []
    if levels is not None:
        if isinstance(chunks, dict) and "level" in chunks:
            level_axes = [AxisDefinition(["levelist"], Chunking.SINGLE_VALUE)]
        else:
            level_axes = [AxisDefinition(["levelist"], Chunking.NONE)]

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


def add_healpix_coordinates(ds):
    """Add lon, lat and bounds coordinates for HEALPix grid."""

    ncell = ds.sizes["cell"]
    nside = int(np.sqrt(ncell / 12))

    hp = astropy_healpix.HEALPix(nside=nside, order="nested")
    lon, lat = hp.healpix_to_lonlat(np.arange(ncell))
    lon_vals = lon.to_value("deg")
    lat_vals = lat.to_value("deg")

    lon_b, lat_b = hp.boundaries_lonlat(np.arange(ncell), step=1)
    lon_b_vals = lon_b.to_value("deg")
    lat_b_vals = lat_b.to_value("deg")

    ds = ds.assign_coords(
        {
            "lon": (("cell",), lon_vals, {"units": "degrees_east", "long_name": "longitude"}),
            "lat": (("cell",), lat_vals, {"units": "degrees_north", "long_name": "latitude"}),
            "lon_bounds": (("cell", "vertices"), lon_b_vals),
            "lat_bounds": (("cell", "vertices"), lat_b_vals),
        }
    )
    return ds


def add_lonlat_coordinates(ds):
    """Add coordinates for lonlat grids."""

    if "cell" not in ds.dims:
        return ds

    # Guess dimensions from the idea that
    # 1) the number of lons is even,
    # 2) the number of lats is approximately half of the number of lons

    ncell = ds.sizes["cell"]
    nlon = int(np.sqrt(ncell / 2)) * 2
    nlat = int(ncell / nlon)
    if nlon * nlat != ncell:
        return ds  # abort and do not add coordinates

    lon_vals = np.linspace(0, 360, nlon, endpoint=False)
    lat_vals = np.linspace(90, -90, nlat)

    index = pd.MultiIndex.from_product([lat_vals, lon_vals], names=["lat", "lon"])
    mindex_coords = xr.Coordinates.from_pandas_multiindex(index, "cell")
    ds = ds.assign_coords(mindex_coords)
    ds = ds.unstack("cell")

    # Add attributes to coordinates
    ds.lon.attrs.update({"units": "degrees_east", "long_name": "longitude"})
    ds.lat.attrs.update({"units": "degrees_north", "long_name": "latitude"})

    return ds


def add_coordinates(ds, levunits=None, grid_type=None):
    """Add coordinates based on grid type."""
    if levunits:
        ds.level.attrs["units"] = levunits

    if grid_type == "healpix_unstructured":
        grid_type = "unknown"
        if "cell" in ds.dims:
            ncell = ds.sizes["cell"]
            nside = int(np.round(np.sqrt(ncell / 12)))
            if 12 * nside**2 == ncell and (nside & (nside - 1)) == 0:
                grid_type = "healpix"

    if grid_type == "healpix":
        return add_healpix_coordinates(ds)
    elif grid_type == "lonlat":
        return add_lonlat_coordinates(ds)

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
):
    """Open a Climate DT FDB selection as an xarray.Dataset using z3fdb.

    Constructs axis definitions and time coordinate accordingly.
    Returns a lazy (dask-backed) xr.Dataset.

    Args:
        request (dict): Dictionary containing the FDB request.
        variables (list, optional): List of variables to request. Defaults to None.
        levels (list, optional): List of levels to request. Defaults to None.
        config (str, optional): Path to the configuration file. Defaults to "./config.yaml".
        years (range, optional): Range of years to request. Defaults to None.
        startdate (str, optional): Start date of the data. Defaults to None.
        enddate (str, optional): End date of the data. Defaults to None.
        data_start_date (str, optional): Start date of the dataset. Defaults to None.
        data_end_date (str, optional): End date of the dataset. Defaults to None.
        freq (str, optional): Frequency of the data. Defaults to "MS".
        chunks (dict, optional): Chunking configuration for the zarr array.
            At the moment it only supports one key 'level', when this is provided,
            the level axis is chunked, otherwise it is not chunked.
            The value of the chunk size for the level axis is ignored.
            The time axis is always chunked as single values. Defaults to None.
        level_values (list, optional): List of physical values of levels. Defaults to None.
        grid (str, optional): Name of the grid. Defaults to None.

    Returns:
        xarray.Dataset: Lazy (dask-backed) xr.Dataset.
    """

    _check_availability()

    if grid and "lon-lat" in grid:
        grid_type = "lonlat"
    else:
        grid_type = "healpix_unstructured"

    if variables:
        request["param"] = variables
    else:
        variables = request.get("param", None)

    if not isinstance(variables, (list, tuple, range)):
        variables = [variables]

    levelist = request.get("levelist")
    if levels:
        request["levelist"] = levels
    else:
        levels = levelist

    if not level_values:
        level_values = levels

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
    store._serialized_axes = [(axis.keys, axis.chunking.name) for axis in axes]
    store._extractor_type_str = ExtractorType.GRIB.name

    zarr_arr = zarr.open_array(store, mode="r", zarr_format=3, use_consolidated=False)

    ds = to_dataset(
        zarr_arr,
        keys=variables,
        start_date=start,
        freq=pd_freq,
        levels=level_values,
    )

    ds = add_coordinates(ds, levunits=levunits, grid_type=grid_type)

    ds.attrs.update(
        {
            "mars_request": "; ".join(mars_list) if len(mars_list) > 1 else mars_list[0],
        }
    )

    return ds

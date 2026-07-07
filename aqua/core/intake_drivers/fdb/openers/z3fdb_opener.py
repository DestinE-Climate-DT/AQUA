# Derived from the fdb-xarray library
# https://github.com/koldunovn/fdb-xarray

import copyreg

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

    builder = SimpleStoreBuilder(config)
    axes = []
    for keys, chunking_str in serialized_axes:
        chunking = getattr(Chunking, chunking_str)
        axes.append(AxisDefinition(keys, chunking))
    extractor_type = getattr(ExtractorType, extractor_type_str)
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


def _build_axes(request, freq, levels, years, start_date=None, end_date=None, chunks=None):

    req = {}

    if years is None and (start_date is None or end_date is None):
        years = request["year"]
        if not isinstance(years, (list, tuple, range)):
            years = [years]
        years = [str(y) for y in years]

    if start_date is not None or end_date is not None:
        if start_date is None or end_date is None:
            raise ValueError("provide both start_date and end_date")

        d0 = _mars_date(start_date)
        d1 = _mars_date(end_date)

        if freq not in ("h", "D"):
            # recover years from dates. If end_date does not end on 1231, we exclude that year.
            # TODO allow incomplete years
            if d1[-4:] != "1231":
                years = range(int(d0[:4]), int(d1[:4]))
            else:
                years = range(int(d0[:4]), int(d1[:4]) + 1)

        iso_start = f"{d0[:4]}-{d0[4:6]}-{d0[6:8]}"
    else:
        ys = list(years)
        d0 = f"{ys[0]}0101"
        d1 = f"{ys[-1]}1231"
        iso_start = f"{ys[0]}-01-01"

    if freq == "h":
        req["date"] = f"{d0}/to/{d1}/by/1"
        req["time"] = "0000/to/2300/by/1"
        time_axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        pd_freq = "1h"
        start = iso_start
    elif freq == "D":
        # Daily data still uses a time=0000 key with merged (date,time) axis.
        req["date"] = f"{d0}/to/{d1}/by/1"
        req["time"] = "0000"
        time_axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        pd_freq = "1D"
        start = iso_start
    elif freq == "MS":
        years = list(years)
        req["year"] = "/".join(str(y) for y in years)
        req["month"] = "1/2/3/4/5/6/7/8/9/10/11/12"
        time_axes = [AxisDefinition(["year", "month"], Chunking.SINGLE_VALUE)]
        pd_freq = "MS"
        start = iso_start
    else:
        raise ValueError(f"Unknown freq {freq!r}")

    level_axes = []
    if levels is not None:
        if isinstance(chunks, dict) and "level" in chunks:
            level_axes = [AxisDefinition(["levelist"], Chunking.SINGLE_VALUE)]
        else:
            level_axes = [AxisDefinition(["levelist"], Chunking.NONE)]

    request.update(req)

    axes = time_axes + [AxisDefinition(["param"], Chunking.SINGLE_VALUE)] + level_axes

    # Create mars request as a string
    mars = ",".join(f"{k}=" + ("/".join(map(str, v)) if isinstance(v, list) else str(v)) for k, v in request.items())

    return mars, axes, pd_freq, start


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


def add_coordinates(ds, levunits=None):
    """Add coordinates based on grid type."""

    if "cell" in ds.dims:
        is_healpix = True
    else:
        is_healpix = False

    if is_healpix:
        ncell = ds.sizes["cell"]
        is_healpix = (
            ncell % 12 == 0 and (ncell // 12).bit_length() - 1 == np.log2(ncell // 12) and (ncell // 12).bit_length() % 2 == 1
        )

    if not is_healpix:
        raise NotImplementedError("Only HEALPix grids are supported")

    if levunits:
        ds.level.attrs["units"] = levunits

    return add_healpix_coordinates(ds)


def open_z3fdb(
    request,
    variables=None,
    levels=None,
    config="./config.yaml",
    years=None,
    startdate=None,
    enddate=None,
    data_start_date=None,
    data_end_date=None,
    freq="MS",
    chunks=None,
    level_values=None,
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

    Returns:
        xarray.Dataset: Lazy (dask-backed) xr.Dataset.
    """

    _check_availability()

    if variables:
        request["param"] = variables
    else:
        variables = request.get("param", None)

    if not isinstance(variables, (list, tuple, range)):
        variables = [variables]

    if levels:
        request["levelist"] = levels
    else:
        levels = request.get("levelist", None)

    # Set level units based on level type
    levtype = request.get("levtype", None).lower()
    if levtype == "pl":
        levunits = "hPa"
    elif levtype == "o3d" or levtype == "hl":
        levunits = "m"
    else:
        levunits = None

    if not level_values:
        level_values = levels

    # if years is not defined and startdate and enddate are not defined
    # then we use data_start_date to define startdate

    if years is None:
        if startdate is None:
            startdate = data_start_date
        if enddate is None:
            enddate = data_end_date

    # print("Calling with ", freq, levels, years, startdate, enddate )
    mars, axes, pd_freq, start = _build_axes(request, freq, levels, years, startdate, enddate, chunks)

    # Create zarr store
    builder = SimpleStoreBuilder(config)
    builder.add_part(mars, axes, ExtractorType.GRIB)
    store = builder.build()

    # Attach serialization attributes for pickling/Dask support
    store._config = config
    store._mars = mars
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

    ds = add_coordinates(ds, levunits)

    ds.attrs.update(
        {
            "mars_request": mars,
        }
    )

    return ds

"""Selection, grids and serialization through the public z3fdb reading path."""

import os
import pickle
import subprocess
import sys
from copy import deepcopy

import dask.array as da
import eccodes as ecc
import numpy as np
import pytest
import xarray as xr

from aqua.core.exceptions import NoDataError
from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import (
    open_z3fdb,
    rebuild_fdb_zarr_store,
    reduce_fdb_zarr_store,
    z3fdb_available,
)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "levelist, selected, physical, expected",
    [
        ([1000, 900, 800], None, None, [1000, 900, 800]),
        ([1000, 900, 800], [900, 800], None, [900, 800]),
        ([1000, 900, 800], [800, 1000], None, [800, 1000]),
        ([1000, 900, 800], [900, 800], [999.99, 899.99, 799.99], [899.99, 799.99]),
        ([1000, 900, 800], [800, 1000], [999.99, 899.99, 799.99], [799.99, 999.99]),
        ([1000, 900, 800], 900, None, [900]),
        (1000, None, None, [1000]),
        (1000, 1000, 999.99, [999.99]),
        (0, 0, 0, [0]),
    ],
)
def test_level_selection_across_storage(z3fdb_store, levelist, selected, physical, expected):
    """Subsets and reordering must keep physical coordinates aligned with requested level IDs."""
    fdb = z3fdb_store((1, 1, len(expected), 12))
    request = {"param": [130], "levtype": "pl", "levelist": levelist}
    original = deepcopy(request)
    result = IntakeFDBSource(
        request,
        engine="z3fdb",
        level=selected,
        metadata={"levels": physical} if physical is not None else {},
        data_start_date="20200101",
        data_end_date="20200101T0200",
        bridge_start_date="20200101T0100",
        bridge_end_date="20200101T0100",
    ).to_dask()
    assert result.level.to_numpy().tolist() == expected
    assert result.level.attrs["units"] == "hPa"
    ids = selected if selected is not None else levelist
    ids = ids if isinstance(ids, list) else [ids]
    assert [call.request["levelist"] for call in fdb.calls] == [ids] * 3
    assert result.sizes["time"] == 3
    assert request == original
    assert isinstance(result.var130.data, da.Array)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "mode, bounds, hpc_expver, expected_stores",
    [
        ("paths", ("20200101T0100", "20200101T0100"), "a0h3", ["hpc", "bridge", "hpc"]),
        ("homes", ("20200101T0100", "20200101T0100"), None, ["hpc", "bridge", "hpc"]),
        ("fallback", ("20200101T0100", "20200101T0100"), "a0h3", ["hpc", "hpc", "hpc"]),
        ("override", ("20200101T0100", "20200101T0100"), "a0h3", ["hpc", "hpc", "hpc"]),
        ("paths", (None, None), "a0h3", ["hpc"]),
        ("paths", ("complete", "complete"), "a0h3", ["bridge"]),
    ],
)
def test_repeated_reads_preserve_routing(z3fdb_store, mode, bounds, hpc_expver, expected_stores):
    """Repeated reads must retain path metadata and apply experiment overrides only outside bridge."""
    fdb = z3fdb_store((1 if len(expected_stores) == 3 else 3, 1, 12))
    metadata = fdb.metadata.copy()
    if mode == "homes":
        metadata = {
            key.replace("path", "home"): os.path.dirname(os.path.dirname(os.path.dirname(value)))
            for key, value in metadata.items()
        }
    elif mode == "fallback":
        metadata.pop("fdb_path_bridge")
    original = deepcopy(metadata)
    request = {"param": [130], "expver": "0001"}
    source = IntakeFDBSource(
        request,
        metadata=metadata,
        engine="z3fdb",
        hpc_expver=hpc_expver,
        config_fdb=fdb.configs["hpc"] if mode == "override" else None,
        data_start_date="20200101",
        data_end_date="20200101T0200",
        bridge_start_date=bounds[0],
        bridge_end_date=bounds[1],
    )
    first, second = source.to_dask(), source.to_dask()
    assert [call.config for call in fdb.calls] == [fdb.configs[name] for name in expected_stores] * 2
    expected_expvers = (
        [hpc_expver or "0001", "0001", hpc_expver or "0001"]
        if len(expected_stores) == 3
        else ["0001" if bounds[0] else hpc_expver]
    )
    assert [call.request["expver"] for call in fdb.calls] == expected_expvers * 2
    assert request == {"param": [130], "expver": "0001"}
    assert metadata == source.metadata == original
    xr.testing.assert_equal(first, second)
    assert "Spatial coordinates decoded" in first.attrs["history"]
    assert "Spatial coordinates decoded" in second.attrs["history"]


@pytest.mark.aqua
@pytest.mark.parametrize("ordering", [0, 1], ids=["ring", "nested"])
def test_healpix_grid_from_grib(z3fdb_store, fdb_grib_message, ordering):
    """Pixels must retain their values, dimensions and the GRIB ordering rather than guessed ordering."""
    message = fdb_grib_message(Nside=2, ordering=ordering)
    handle = ecc.codes_new_from_message(message)
    try:
        expected_lon = ecc.codes_get_array(handle, "longitudes")
        expected_lat = ecc.codes_get_array(handle, "latitudes")
    finally:
        ecc.codes_release(handle)
    fdb = z3fdb_store(
        (1, 1, 48),
        message=message,
        values=np.arange(48, dtype="float32").reshape(1, 1, 48),
    )
    result = open_z3fdb({"param": [130]}, freq="h", startdate="20200101", enddate="20200101")
    assert result.var130.dims == ("time", "ncells")
    assert "cell" not in result.coords and "ncells" not in result.coords
    assert result.lon_bounds.shape == result.lat_bounds.shape == (48, 4)
    assert result.lon.to_numpy() == pytest.approx(expected_lon)
    assert result.lat.to_numpy() == pytest.approx(expected_lat)
    assert result.var130.isel(time=0).compute().to_numpy().tolist() == list(range(48))
    assert result.lon.attrs["units"] == "degrees_east"
    assert result.attrs["history"]
    assert all(stream.closed for stream in fdb.streams)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "grid, expected_lon, expected_lat, expected_values",
    [
        ({}, [0, 90, 180, 270], [90, 0, -90], [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]]),
        (
            {"latitudeOfFirstGridPointInDegrees": -90, "latitudeOfLastGridPointInDegrees": 90, "jScansPositively": 1},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[8, 9, 10, 11], [4, 5, 6, 7], [0, 1, 2, 3]],
        ),
        (
            {
                "latitudeOfFirstGridPointInDegrees": 89.5,
                "latitudeOfLastGridPointInDegrees": -89.5,
                "jDirectionIncrementInDegrees": 89.5,
            },
            [0, 90, 180, 270],
            [89.5, 0, -89.5],
            [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]],
        ),
        (
            {
                "latitudeOfFirstGridPointInDegrees": 20,
                "latitudeOfLastGridPointInDegrees": 22,
                "jDirectionIncrementInDegrees": 1,
                "jScansPositively": 1,
            },
            [0, 90, 180, 270],
            [22, 21, 20],
            [[8, 9, 10, 11], [4, 5, 6, 7], [0, 1, 2, 3]],
        ),
        (
            {"longitudeOfFirstGridPointInDegrees": 45, "longitudeOfLastGridPointInDegrees": 315},
            [45, 135, 225, 315],
            [90, 0, -90],
            [[0, 1, 2, 3], [4, 5, 6, 7], [8, 9, 10, 11]],
        ),
        (
            {"longitudeOfFirstGridPointInDegrees": 270, "longitudeOfLastGridPointInDegrees": 180},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[1, 2, 3, 0], [5, 6, 7, 4], [9, 10, 11, 8]],
        ),
        (
            {"longitudeOfFirstGridPointInDegrees": 270, "longitudeOfLastGridPointInDegrees": 0, "iScansNegatively": 1},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[3, 2, 1, 0], [7, 6, 5, 4], [11, 10, 9, 8]],
        ),
        (
            {"jPointsAreConsecutive": 1},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[0, 3, 6, 9], [1, 4, 7, 10], [2, 5, 8, 11]],
        ),
        (
            {"alternativeRowScanning": 1},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[0, 1, 2, 3], [7, 6, 5, 4], [8, 9, 10, 11]],
        ),
        (
            {"jPointsAreConsecutive": 1, "alternativeRowScanning": 1, "latitudeOfLastGridPointInDegrees": 90},
            [0, 90, 180, 270],
            [90, 0, -90],
            [[0, 5, 6, 11], [1, 4, 7, 10], [2, 3, 8, 9]],
        ),
    ],
    ids=[
        "north-first",
        "south-first",
        "half-degree",
        "north-only",
        "shifted",
        "wrapped",
        "westward",
        "column-first",
        "alternating-rows",
        "alternating-columns",
    ],
)
def test_lonlat_grid_scan_order(z3fdb_store, fdb_grib_message, grid, expected_lon, expected_lat, expected_values):
    """GRIB extents and scan flags must place each field value at its actual latitude and longitude."""
    keys = {
        "Ni": 4,
        "Nj": 3,
        "latitudeOfFirstGridPointInDegrees": 90,
        "latitudeOfLastGridPointInDegrees": -90,
        "longitudeOfFirstGridPointInDegrees": 0,
        "longitudeOfLastGridPointInDegrees": 270,
        "iDirectionIncrementInDegrees": 90,
        "jDirectionIncrementInDegrees": 90,
        **grid,
    }
    message = fdb_grib_message("regular_ll", **keys)
    fdb = z3fdb_store((1, 1, 12), message=message, values=np.arange(12, dtype="float32").reshape(1, 1, 12))
    result = open_z3fdb({"param": [130]}, freq="h", startdate="20200101", enddate="20200101")
    assert result.var130.dims == ("time", "lat", "lon")
    assert isinstance(result.var130.data, da.Array)
    assert result.lon.to_numpy().tolist() == expected_lon
    assert result.lat.to_numpy().tolist() == expected_lat
    assert result.var130.isel(time=0).compute().to_numpy().tolist() == expected_values
    assert result.lon.attrs["units"] == "degrees_east"
    assert result.lat.attrs["units"] == "degrees_north"
    assert "Spatial coordinates decoded" in result.attrs["history"]
    fdb.retrieve.assert_called_once()


@pytest.mark.aqua
@pytest.mark.parametrize(
    "grid_type, shape, error, match",
    [
        (None, (1, 1, 12), NoDataError, "No GRIB"),
        ("rotated_ll", (1, 1, 12), ValueError, "only HEALPix"),
        ("reduced_gg", (1, 1, 12), ValueError, "only HEALPix"),
        ("unstructured_grid", (1, 1, 12), ValueError, "only HEALPix"),
        ("healpix", (1, 1, 48), ValueError, "point count"),
        ("regular_ll", (1, 1, 5), ValueError, "point count"),
    ],
)
def test_invalid_grid_rejected(z3fdb_store, fdb_grib_message, grid_type, shape, error, match):
    """Missing or incompatible grid metadata must fail rather than assign plausible coordinates."""
    fdb = z3fdb_store(shape, message=b"" if grid_type is None else fdb_grib_message(grid_type))
    with pytest.raises(error, match=match):
        open_z3fdb({"param": [130]}, freq="h", startdate="20200101", enddate="20200101")
    assert all(stream.closed for stream in fdb.streams)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "chunks, count, expected",
    [
        (None, 2, (2,)),
        ({"vertical": 2}, 4, (2, 2)),
        ({"level": "2"}, 4, (2, 2)),
        ({"vertical": 1}, 2, (1, 1)),
        ({"vertical": 2}, 2, (2,)),
        ({"vertical": 2}, 3, (1, 1, 1)),
    ],
)
def test_vertical_chunks(z3fdb_store, caplog, chunks, count, expected):
    """Only divisors of the selected level count may form multi-level native chunks."""
    z3fdb_store((1, 1, count, 12))
    result = open_z3fdb(
        {"param": [130], "levelist": list(range(count))}, freq="h", startdate="20200101", enddate="20200101", chunks=chunks
    )
    assert result.var130.chunksizes["level"] == expected
    if count == 3:
        assert "falling back to SINGLE_VALUE" in caplog.text


@pytest.mark.aqua
def test_netcdf_and_sample_request(z3fdb_store, tmp_path):
    """Filter unsupported keys, probe one field and preserve requests and history when writing a slice."""
    fdb = z3fdb_store((2, 2, 2, 12))
    request = {
        "param": [167, 168],
        "levtype": "pl",
        "levelist": [900, 800],
        "expver": "0001",
        "expid": "ignored",
        "unknown": "ignored",
    }
    original = deepcopy(request)
    result = open_z3fdb(request, config=fdb.configs["hpc"], freq="h", startdate="20200101T2300", enddate="20200102")
    assert request == original
    assert all("expid" not in call.request and "unknown" not in call.request for call in fdb.calls)
    fdb.retrieve.assert_called_once_with(
        {"param": "167", "levtype": "pl", "levelist": "900", "expver": "0001", "date": "20200101", "time": "2300"}
    )
    assert fdb.stores[0]._mars == [call.request for call in fdb.calls]
    sliced = result.isel(time=0)
    output = os.path.join(tmp_path, "z3fdb.nc")
    sliced.to_netcdf(output)
    with xr.open_dataset(output) as restored:
        xr.testing.assert_equal(restored, sliced)
        assert restored.attrs["history"] == result.attrs["history"]
        assert restored.attrs["mars_request"] == (
            "param=167/168,levtype=pl,levelist=900/800,expver=0001,date=20200101,time=2300; "
            "param=167/168,levtype=pl,levelist=900/800,expver=0001,date=20200102,time=0000"
        )


CMIP6_MARS_REQ = {
    "class": "d1",
    "dataset": "climate-dt",
    "activity": "CMIP6",
    "experiment": "hist",
    "generation": 1,
    "model": "IFS-NEMO",
    "realization": 1,
    "resolution": "standard",
    "expver": "a0h3",
    "type": "fc",
    "stream": "clte",
    "date": "19900101",
    "time": "0000",
    "param": 164,
    "levtype": "sfc",
}


FDB_HOME = "/app"


@pytest.mark.fdb
@pytest.mark.skipif(not z3fdb_available, reason="z3fdb not available")
class TestZ3fdbStore:
    """FdbZarrStore building and pickling: needs the z3fdb library and an FDB schema."""

    def test_z3fdb_store_pickling(self) -> None:
        """Test FdbZarrStore custom pickling and unpickling."""
        from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

        builder = SimpleStoreBuilder(None)
        axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        builder.add_part(CMIP6_MARS_REQ, axes, ExtractorType.GRIB)
        try:
            store = builder.build()
        except RuntimeError as e:
            if "schema" in str(e) or "no data found" in str(e):
                pytest.skip(f"FDB schema or test data not available: {e}")
            raise

        # Attach serialization attributes to verify pickling
        store._config = None
        store._mars = [CMIP6_MARS_REQ]
        store._serialized_axes = [(axis.keys, axis.chunking.name) for axis in axes]
        store._extractor_type_str = ExtractorType.GRIB.name

        pickled = pickle.dumps(store)
        unpickled = pickle.loads(pickled)
        assert unpickled is not None

    def test_z3fdb_rebuild_parameters(self) -> None:
        """Test rebuild_fdb_zarr_store parameters and path logic."""
        serialized_axes = [(["date", "time"], "SINGLE_VALUE")]
        # Test with mars as single dict
        try:
            store1 = rebuild_fdb_zarr_store(
                config=None, mars=CMIP6_MARS_REQ, serialized_axes=serialized_axes, extractor_type_str="GRIB"
            )
        except RuntimeError as e:
            if "schema" in str(e) or "no data found" in str(e):
                pytest.skip(f"FDB schema or test data not available: {e}")
            raise
        assert store1 is not None

        # Test with config path and mars as list
        store2 = rebuild_fdb_zarr_store(
            config=FDB_HOME, mars=[CMIP6_MARS_REQ], serialized_axes=serialized_axes, extractor_type_str="GRIB"
        )
        assert store2 is not None

    def test_z3fdb_missing_attrs_raise(self) -> None:
        """Test reduce_fdb_zarr_store raises TypeError on missing attrs."""
        from z3fdb import AxisDefinition, Chunking, ExtractorType, SimpleStoreBuilder

        builder = SimpleStoreBuilder(None)
        axes = [AxisDefinition(["date", "time"], Chunking.SINGLE_VALUE)]
        builder.add_part(CMIP6_MARS_REQ, axes, ExtractorType.GRIB)
        try:
            store = builder.build()
        except RuntimeError as e:
            if "schema" in str(e) or "no data found" in str(e):
                pytest.skip(f"FDB schema or test data not available: {e}")
            raise

        with pytest.raises(TypeError, match="Cannot pickle FdbZarrStore: missing serialization attributes"):
            reduce_fdb_zarr_store(store)


@pytest.mark.aqua
def test_z3fdb_without_gsv():
    """Import and use the z3fdb coordinate path when GSV cannot be imported."""
    if not z3fdb_available:
        pytest.skip("z3fdb and pyfdb are required to check GSV-independent access")
    code = """
import os
import sys
sys.modules['gsv'] = None
os.environ.pop('GRID_DEFINITION_PATH', None)
import xarray as xr
from aqua.core.intake_drivers.fdb.openers import z3fdb_opener
assert z3fdb_opener.z3fdb_available
result = z3fdb_opener.add_coordinates(
    xr.Dataset(coords={'cell': range(12)}), {'grid_type': 'healpix', 'Nside': 1, 'ordering': 0}
)
assert result.lon.size == 12
"""
    result = subprocess.run(
        [sys.executable, "-c", code], cwd=os.path.dirname(os.path.dirname(__file__)), capture_output=True, text=True
    )
    assert result.returncode == 0, result.stderr

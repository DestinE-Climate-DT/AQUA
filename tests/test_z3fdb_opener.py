"""Tests for the z3fdb opener in AQUA: MARS requests, coordinates and the pickling of the FDB Zarr store."""

import os
import pickle
from unittest.mock import Mock

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr

from aqua.core.intake_drivers.fdb.openers import z3fdb_opener
from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import (
    _build_zarr_axes,
    add_coordinates,
    add_lonlat_coordinates,
    rebuild_fdb_zarr_store,
    reduce_fdb_zarr_store,
    z3fdb_available,
)
from aqua.core.util import to_list

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


@pytest.fixture
def fake_fdb(monkeypatch):
    """Replace the FDB access of open_z3fdb with an in-memory Zarr array of shape (time, param, cell) = (2, nparam, 12).

    Returns the mocked store builder: its ``add_part`` calls are the MARS requests sent to FDB.
    """
    monkeypatch.setattr(z3fdb_opener, "_check_availability", Mock())
    monkeypatch.setattr(z3fdb_opener, "_build_zarr_axes", Mock(return_value=[]))
    builder = Mock()
    monkeypatch.setattr(z3fdb_opener, "SimpleStoreBuilder", Mock(return_value=builder), raising=False)
    monkeypatch.setattr(z3fdb_opener, "ExtractorType", Mock(), raising=False)

    def open_array(*args, **kwargs):
        """A zero array with one param slice per requested variable."""
        nparam = len(to_list(builder.add_part.call_args.args[0]["param"]))
        return zarr.zeros(shape=(2, nparam, 12), chunks=(1, 1, 12), dtype="float32")

    monkeypatch.setattr(z3fdb_opener.zarr, "open_array", open_array)
    return builder


@pytest.mark.aqua
class TestZ3fdbRequests:
    """MARS request building and coordinate handling: pure logic, FDB access mocked."""

    @pytest.mark.parametrize("extra_keys", [{}, {"expid": "o031", "unknown": "ignored"}], ids=["valid-keys", "extra-keys"])
    @pytest.mark.parametrize(
        "startdate, enddate, expected_request",
        [
            (
                "2020-01-01T00:00:00",
                "2020-01-01T01:00:00",
                "param=167/168,levtype=sfc,date=20200101,time=0000/to/0100/by/1",
            ),
            (
                "2020-01-01T23:00:00",
                "2020-01-02T00:00:00",
                "param=167/168,levtype=sfc,date=20200101,time=2300/to/2300/by/1; "
                "param=167/168,levtype=sfc,date=20200102,time=0000/to/0000/by/1",
            ),
        ],
        ids=["single-request", "multiple-requests"],
    )
    def test_z3fdb_mars_request_netcdf(self, fake_fdb, tmp_path, startdate, enddate, expected_request, extra_keys):
        """Only valid MARS keys reach FDB, serialization and NetCDF metadata."""
        request = {"param": [167, 168], "levtype": "sfc", "expver": "0001", **extra_keys}
        original_request = request.copy()
        data = z3fdb_opener.open_z3fdb(
            request=request,
            startdate=startdate,
            enddate=enddate,
            freq="h",
        )
        assert request == original_request
        requests = [call.args[0] for call in fake_fdb.add_part.call_args_list]
        assert all("expid" not in req and "unknown" not in req and req["expver"] == "0001" for req in requests)
        assert fake_fdb.build.return_value._mars == requests
        expected_request = expected_request.replace("levtype=sfc,", "levtype=sfc,expver=0001,")
        assert isinstance(data["var167"].data, da.Array)
        data.attrs["history"] = "z3fdb NetCDF regression test"
        sliced = data.isel(time=0)
        output = os.path.join(tmp_path, "z3fdb.nc")
        sliced.to_netcdf(output)

        with xr.open_dataset(output) as restored:
            assert restored.attrs["mars_request"] == expected_request
            assert restored.attrs["history"] == data.attrs["history"]
            xr.testing.assert_equal(restored, sliced)

    @pytest.mark.parametrize(
        "freq, request_keys, dates, expected_parts, expected_times",
        [
            pytest.param(
                "D",
                {"year": 2020},
                {},
                [{"date": "20200101/to/20201231/by/1", "time": "0000"}],
                ["2020-01-01", "2020-01-02"],
                id="daily-year-from-request",
            ),
            pytest.param(
                "D",
                {"year": [2021, 2020]},
                {},
                [{"date": "20200101/to/20211231/by/1", "time": "0000"}],
                ["2020-01-01", "2020-01-02"],
                id="daily-years-from-request",
            ),
            pytest.param(
                "D",
                {"year": 1999, "month": 5},
                {"startdate": "2020-05-01", "enddate": "2020-05-10"},
                [{"date": "20200501/to/20200510/by/1", "time": "0000"}],
                ["2020-05-01", "2020-05-02"],
                id="daily-dates-drop-year-month",
            ),
            pytest.param(
                "D",
                {},
                {"startdate": "2020-05-01", "enddate": "2020-05-01"},
                [{"date": "20200501", "time": "0000"}],
                ["2020-05-01", "2020-05-02"],
                id="daily-single-day",
            ),
            pytest.param(
                "h",
                {"year": 1999},
                {"startdate": "2020-01-01T12:00:00", "enddate": "2020-01-03T12:00:00"},
                [
                    {"date": "20200101", "time": "1200/to/2300/by/1"},
                    {"date": "20200102", "time": "0000/to/2300/by/1"},
                    {"date": "20200103", "time": "0000/to/1200/by/1"},
                ],
                ["2020-01-01T12:00", "2020-01-01T13:00"],
                id="hourly-partial-days-one-full-day",
            ),
            pytest.param(
                "h",
                {},
                {"startdate": "2020-01-01T12:00:00", "enddate": "2020-01-04T12:00:00"},
                [
                    {"date": "20200101", "time": "1200/to/2300/by/1"},
                    {"date": "20200102/to/20200103/by/1", "time": "0000/to/2300/by/1"},
                    {"date": "20200104", "time": "0000/to/1200/by/1"},
                ],
                ["2020-01-01T12:00", "2020-01-01T13:00"],
                id="hourly-partial-days-full-days",
            ),
            pytest.param(
                "MS",
                {"date": "19000101", "time": "1200"},
                {"startdate": "2020-01-01", "enddate": "2021-12-01"},
                [{"year": "2020/2021", "month": "1/2/3/4/5/6/7/8/9/10/11/12"}],
                ["2020-01-01", "2020-02-01"],
                id="monthly-same-months-grouped-drop-date-time",
            ),
            pytest.param(
                "MS",
                {},
                {"startdate": "2020-01-01", "enddate": "2021-02-01"},
                [{"year": "2020", "month": "1/2/3/4/5/6/7/8/9/10/11/12"}, {"year": "2021", "month": "1/2"}],
                ["2020-01-01", "2020-02-01"],
                id="monthly-different-months",
            ),
        ],
    )
    def test_z3fdb_mars_requests(self, fake_fdb, freq, request_keys, dates, expected_parts, expected_times):
        """The exact MARS requests and time axis built for each frequency: fails on a wrong split or a key not removed."""
        data = z3fdb_opener.open_z3fdb(request={"param": 129, **request_keys}, freq=freq, **dates)

        requests = [call.args[0] for call in fake_fdb.add_part.call_args_list]
        assert requests == [{"param": 129, **part} for part in expected_parts]
        assert fake_fdb.extend_on_axis.called == (len(expected_parts) > 1)
        assert data.time.values.tolist() == pd.to_datetime(expected_times).values.tolist()

    @pytest.mark.parametrize(
        "kwargs, error, match",
        [
            ({"startdate": "2020-01-01"}, ValueError, "provide both start_date and end_date"),
            ({"data_end_date": "2020-01-01"}, ValueError, "provide both start_date and end_date"),
            ({"startdate": "2020-01-01", "enddate": "2020-01-02", "freq": "W"}, ValueError, "Unknown freq 'W'"),
        ],
    )
    def test_z3fdb_invalid_dates(self, fake_fdb, kwargs, error, match):
        """Half-open date ranges and unknown frequencies are rejected before any FDB access."""
        with pytest.raises(error, match=match):
            z3fdb_opener.open_z3fdb(request={"param": 129, "year": 2020}, **{"freq": "D", **kwargs})
        assert not fake_fdb.add_part.called

    def test_z3fdb_healpix_coordinates(self) -> None:
        """48 cells are a nested nside=2 HEALPix grid with its centers and bounds; 50 cells get no coordinates."""
        ds_out = add_coordinates(xr.Dataset(coords={"cell": np.arange(48)}), grid_type="healpix_unstructured")
        assert ds_out.lon.values[[0, 1, 47]] == pytest.approx([45.0, 67.5, 315.0])
        assert ds_out.lat.values[[0, 1, 47]] == pytest.approx([19.47122063, 41.8103149, -19.47122063])
        assert ds_out.lon_bounds.shape == (48, 4)
        assert ds_out.lat.attrs["units"] == "degrees_north"

        ds_invalid = xr.Dataset(coords={"cell": np.arange(50)})
        assert "lon" not in add_coordinates(ds_invalid, grid_type="healpix_unstructured").coords

    def test_z3fdb_lonlat_coordinates(self) -> None:
        """8 cells are unstacked into a 2x4 lon-lat grid starting at (90N, 0E); 5 cells are left untouched."""
        ds = xr.Dataset({"var": ("cell", np.arange(8.0))})
        ds_out = add_coordinates(ds, grid_type="lonlat")
        assert ds_out["var"].dims == ("lat", "lon")
        assert ds_out.lon.values.tolist() == [0, 90, 180, 270]
        assert ds_out["var"].sel(lat=90).values.tolist() == [0, 1, 2, 3]
        assert ds_out["var"].sel(lat=-90).values.tolist() == [4, 5, 6, 7]
        assert ds_out.lon.attrs["units"] == "degrees_east"

        ds_mismatch = xr.Dataset(coords={"cell": np.arange(5)})
        assert add_lonlat_coordinates(ds_mismatch).equals(ds_mismatch)


@pytest.mark.fdb
@pytest.mark.skipif(not z3fdb_available, reason="z3fdb not available")
class TestZ3fdbStore:
    """FdbZarrStore building, pickling and chunking: needs the z3fdb library."""

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

        unpickled = pickle.loads(pickle.dumps(store))
        assert type(unpickled) is type(store)
        assert unpickled._config is None
        assert unpickled._mars == [CMIP6_MARS_REQ]
        assert unpickled._serialized_axes == [(["date", "time"], "SINGLE_VALUE")]
        assert unpickled._extractor_type_str == "GRIB"

    def test_z3fdb_rebuild_parameters(self, monkeypatch) -> None:
        """rebuild_fdb_zarr_store keeps its parameters on the store and exports an FDB home directory as FDB_HOME."""
        monkeypatch.delenv("FDB_HOME", raising=False)
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
        assert (store1._config, store1._mars) == (None, CMIP6_MARS_REQ)
        assert "FDB_HOME" not in os.environ

        # Test with config path and mars as list
        store2 = rebuild_fdb_zarr_store(
            config=FDB_HOME, mars=[CMIP6_MARS_REQ], serialized_axes=serialized_axes, extractor_type_str="GRIB"
        )
        assert (store2._config, store2._mars) == (FDB_HOME, [CMIP6_MARS_REQ])
        assert store2._serialized_axes == serialized_axes
        assert os.environ["FDB_HOME"] == FDB_HOME

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

    def test_build_zarr_axes_chunking(self) -> None:
        """Test time and vertical chunking options in _build_zarr_axes."""
        from z3fdb import Chunking

        # Default without chunks
        axes = _build_zarr_axes("h", levels=[1000, 850])
        assert axes[0].chunking == Chunking.SINGLE_VALUE
        assert axes[0].keys == ["date", "time"]
        assert axes[2].chunking == Chunking.WHOLE_AXIS

        # Time chunking with FixedSizeChunk via dict
        axes_time = _build_zarr_axes("h", levels=None, chunks={"time": 24})
        assert isinstance(axes_time[0].chunking, Chunking.FixedSizeChunk)
        assert axes_time[0].chunking.chunkShape == 24

        # Monthly frequency time chunking
        axes_ms = _build_zarr_axes("MS", levels=None, chunks={"time": 12})
        assert axes_ms[0].keys == ["year", "month"]
        assert isinstance(axes_ms[0].chunking, Chunking.FixedSizeChunk)
        assert axes_ms[0].chunking.chunkShape == 12

        # String chunk representation and single value fallback
        axes_single = _build_zarr_axes("h", levels=None, chunks={"time": 1})
        assert axes_single[0].chunking == Chunking.SINGLE_VALUE
        axes_str = _build_zarr_axes("h", levels=None, chunks={"time": "6"})
        assert isinstance(axes_str[0].chunking, Chunking.FixedSizeChunk)
        assert axes_str[0].chunking.chunkShape == 6

        # Vertical chunking with FixedSizeChunk
        axes_vert = _build_zarr_axes("h", levels=[1000, 850, 700, 500], chunks={"vertical": 2})
        assert isinstance(axes_vert[2].chunking, Chunking.FixedSizeChunk)
        assert axes_vert[2].chunking.chunkShape == 2

        # Vertical chunking with 'level' key and string representation
        axes_vert_str = _build_zarr_axes("h", levels=[1000, 850, 700, 500], chunks={"level": "2"})
        assert isinstance(axes_vert_str[2].chunking, Chunking.FixedSizeChunk)
        assert axes_vert_str[2].chunking.chunkShape == 2

        # Vertical chunk size 1 (SINGLE_VALUE) and equal to n_levels (WHOLE_AXIS)
        axes_vert_1 = _build_zarr_axes("h", levels=[1000, 850], chunks={"vertical": 1})
        assert axes_vert_1[2].chunking == Chunking.SINGLE_VALUE
        axes_vert_all = _build_zarr_axes("h", levels=[1000, 850], chunks={"vertical": 2})
        assert axes_vert_all[2].chunking == Chunking.WHOLE_AXIS

        # Non-divisor vertical chunk size fallback to SINGLE_VALUE
        axes_fallback = _build_zarr_axes("h", levels=[1000, 850, 700], chunks={"vertical": 2})
        assert axes_fallback[2].chunking == Chunking.SINGLE_VALUE

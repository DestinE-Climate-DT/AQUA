"""Tests for z3fdb_opener module in AQUA, specifically focusing on _build_mars_requests."""

import pickle

import numpy as np
import pytest
import xarray as xr

from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import _build_mars_requests

CMIP6_MARS_REQ = (
    "class=d1,dataset=climate-dt,activity=CMIP6,experiment=hist,generation=1,"
    "model=IFS-NEMO,realization=1,resolution=standard,expver=a0h3,type=fc,"
    "stream=clte,date=19900101,time=0000,param=164,levtype=sfc"
)
FDB_HOME = "/app"

pytestmark = pytest.mark.aqua


def test_build_mars_requests_years_and_dates_parsing():
    """Test the years and dates logic in _build_mars_requests.

    Covers lines 98-112 of z3fdb_opener.py:
    - parsing and standardizing single/multiple years from the request when years parameter is None.
    - ValueError raising when only one of start_date and end_date is provided.
    - handling of both start_date and end_date being provided or parsed.
    """
    request = {"year": 2020, "param": 129}

    # 1. years=None, start_date=None, end_date=None, request['year'] is a single int
    mars_list, pd_freq, start = _build_mars_requests(request, freq="D", levels=None, years=None)
    assert pd_freq == "1D"
    assert start == "2020-01-01"
    assert len(mars_list) == 1
    # Since it's daily, request elements like date are mapped, and year/month are removed from the query
    assert "date=20200101/to/20201231/by/1" in mars_list[0]
    assert "year=" not in mars_list[0]

    # 2. years=None, start_date=None, end_date=None, request['year'] is a list
    request_list = {"year": [2020, 2021], "param": 129}
    mars_list, pd_freq, start = _build_mars_requests(request_list, freq="D", levels=None, years=None)
    assert pd_freq == "1D"
    assert start == "2020-01-01"
    assert len(mars_list) == 1
    assert "date=20200101/to/20211231/by/1" in mars_list[0]

    # 3. Raising ValueError when only start_date is provided
    with pytest.raises(ValueError, match="provide both start_date and end_date"):
        _build_mars_requests(request, freq="D", levels=None, years=None, start_date="2020-01-01")

    # 4. Raising ValueError when only end_date is provided
    with pytest.raises(ValueError, match="provide both start_date and end_date"):
        _build_mars_requests(request, freq="D", levels=None, years=None, end_date="2020-01-01")

    # 5. start_date and end_date are both provided (not None)
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="D", levels=None, years=None, start_date="2020-05-01", end_date="2020-05-10"
    )
    assert pd_freq == "1D"
    assert start == "2020-05-01"
    assert len(mars_list) == 1
    assert "date=20200501/to/20200510/by/1" in mars_list[0]


def test_build_mars_requests_hourly_partial_days():
    """Test hourly frequency (_build_mars_requests with freq="h").

    Covers lines 152-156 of z3fdb_opener.py:
    - Appending mid-range full days when day_start_full <= day_end_full.
    - Appending the partial end day if ts_end.hour < 23.
    """
    request = {"param": 129}
    # Case A: ts_start.hour > 0, ts_end.hour < 23, and day_start_full == day_end_full
    # ts_start = 2020-01-01T12:00:00 (hour 12 > 0 -> partial first day)
    # ts_end = 2020-01-03T12:00:00 (hour 12 < 23 -> partial last day)
    # day_start_full = 2020-01-02
    # day_end_full = 2020-01-02
    # day_start_full == day_end_full -> date_val = "20200102"
    # parts:
    # 1. 20200101, 1200/to/2300/by/1
    # 2. 20200102, 0000/to/2300/by/1 (line 152)
    # 3. 20200103, 0000/to/1200/by/1 (line 155)
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="h", levels=None, years=None, start_date="2020-01-01T12:00:00", end_date="2020-01-03T12:00:00"
    )
    assert pd_freq == "1h"
    assert start == "2020-01-01T12:00:00"
    assert len(mars_list) == 3
    assert "date=20200101,time=1200/to/2300/by/1" in mars_list[0]
    assert "date=20200102,time=0000/to/2300/by/1" in mars_list[1]
    assert "date=20200103,time=0000/to/1200/by/1" in mars_list[2]

    # Case B: ts_start.hour > 0, ts_end.hour < 23, and day_start_full < day_end_full
    # ts_start = 2020-01-01T12:00:00
    # ts_end = 2020-01-04T12:00:00
    # day_start_full = 2020-01-02
    # day_end_full = 2020-01-03
    # date_val = "20200102/to/20200103/by/1"
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="h", levels=None, years=None, start_date="2020-01-01T12:00:00", end_date="2020-01-04T12:00:00"
    )
    assert len(mars_list) == 3
    assert "date=20200101,time=1200/to/2300/by/1" in mars_list[0]
    assert "date=20200102/to/20200103/by/1,time=0000/to/2300/by/1" in mars_list[1]
    assert "date=20200104,time=0000/to/1200/by/1" in mars_list[2]


def test_build_mars_requests_daily_and_monthly_start():
    """Test daily and monthly start frequency in _build_mars_requests.

    Covers lines 164-200 of z3fdb_opener.py:
    - Daily frequency (freq="D") with start and end dates different (line 164).
    - Monthly start frequency (freq="MS") logic (lines 170-200):
      - same months grouped across years (lines 186-187, 198).
      - different months not grouped (lines 188-191, 193).
    """
    request = {"param": 129}

    # 1. Daily frequency with different start and end dates (line 164)
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="D", levels=None, years=None, start_date="2020-01-01", end_date="2020-01-05"
    )
    assert pd_freq == "1D"
    assert start == "2020-01-01"
    assert len(mars_list) == 1
    assert "date=20200101/to/20200105/by/1" in mars_list[0]
    assert "time=0000" in mars_list[0]

    # 2. Monthly start frequency with matching month patterns across years (lines 186-187, 198)
    # ts_start = 2020-01-01, ts_end = 2021-12-01 (all 12 months in 2020 and 2021)
    # Should group into year="2020/2021", month="1/2/3/4/5/6/7/8/9/10/11/12"
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="MS", levels=None, years=None, start_date="2020-01-01", end_date="2021-12-01"
    )
    assert pd_freq == "MS"
    assert start == "2020-01-01"
    assert len(mars_list) == 1
    assert "year=2020/2021" in mars_list[0]
    assert "month=1/2/3/4/5/6/7/8/9/10/11/12" in mars_list[0]
    assert "date=" not in mars_list[0]
    assert "time=" not in mars_list[0]

    # 3. Monthly start frequency with non-matching month patterns across years (lines 188-191, 193)
    # ts_start = 2020-01-01, ts_end = 2021-02-01
    # 2020 has months 1..12. 2021 has months 1..2.
    # Should produce 2 requests:
    # - year=2020, month=1/2/3/4/5/6/7/8/9/10/11/12
    # - year=2021, month=1/2
    mars_list, pd_freq, start = _build_mars_requests(
        request, freq="MS", levels=None, years=None, start_date="2020-01-01", end_date="2021-02-01"
    )
    assert pd_freq == "MS"
    assert start == "2020-01-01"
    assert len(mars_list) == 2
    assert "year=2020" in mars_list[0]
    assert "month=1/2/3/4/5/6/7/8/9/10/11/12" in mars_list[0]
    assert "year=2021" in mars_list[1]
    assert "month=1/2" in mars_list[1]


def test_z3fdb_store_pickling() -> None:
    """Test FdbZarrStore custom pickling and unpickling."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

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


def test_z3fdb_rebuild_parameters() -> None:
    """Test rebuild_fdb_zarr_store parameters and path logic."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import rebuild_fdb_zarr_store, z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

    serialized_axes = [(["date", "time"], "SINGLE_VALUE")]
    # Test with mars as string
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


def test_z3fdb_missing_attrs_raise() -> None:
    """Test reduce_fdb_zarr_store raises TypeError on missing attrs."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import reduce_fdb_zarr_store, z3fdb_available

    if not z3fdb_available:
        pytest.skip("z3fdb not available")

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


def test_z3fdb_healpix_unstructured_coordinate_checking() -> None:
    """Test healpix unstructured detection logic."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import add_coordinates

    # ds with cell dimension of 48 (12 * 2^2) -> valid healpix count
    ds_valid = xr.Dataset(coords={"cell": np.arange(48)})
    try:
        ds_out = add_coordinates(ds_valid, grid_type="healpix_unstructured")
        assert "lon" in ds_out.coords
    except (ImportError, ModuleNotFoundError):
        pass

    # ds with invalid cell count
    ds_invalid = xr.Dataset(coords={"cell": np.arange(50)})
    ds_out = add_coordinates(ds_invalid, grid_type="healpix_unstructured")
    assert "lon" not in ds_out.coords


def test_z3fdb_lonlat_mismatch_aborts() -> None:
    """Test add_lonlat_coordinates aborts on mismatching shape."""
    from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import add_lonlat_coordinates

    ds = xr.Dataset(coords={"cell": np.arange(5)})
    ds_out = add_lonlat_coordinates(ds)
    assert ds_out.equals(ds)

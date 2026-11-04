"""Time requests and output coordinates for the z3fdb intake driver."""

from types import SimpleNamespace
from unittest.mock import Mock

import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr
import zarr
from dask.callbacks import Callback

from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.openers import z3fdb_opener
from aqua.core.intake_drivers.fdb.openers.fdb_source import FDBSource
from aqua.core.intake_drivers.fdb.openers.timeutil import FDBTimeMixin

pytestmark = pytest.mark.aqua


@pytest.fixture
def mock_store(monkeypatch):
    """Supply lazy in-memory fields without an FDB installation."""

    def install(nt):
        if not z3fdb_opener.z3fdb_available:
            pytest.skip("z3fdb and pyfdb are required for the virtual store layout.")
        builder = Mock()
        monkeypatch.setattr(z3fdb_opener, "_check_availability", Mock())
        monkeypatch.setattr(z3fdb_opener, "SimpleStoreBuilder", Mock(return_value=builder), raising=False)
        monkeypatch.setattr(z3fdb_opener, "ExtractorType", Mock(), raising=False)
        sizes = nt if isinstance(nt, list) else [nt]
        arrays = [zarr.zeros(shape=(size, 1, 12), chunks=(1, 1, 12), dtype="float32") for size in sizes]
        open_array = Mock(side_effect=arrays) if isinstance(nt, list) else Mock(return_value=arrays[0])
        monkeypatch.setattr(z3fdb_opener.zarr, "open_array", open_array)
        probe = Mock(return_value={"grid_type": "healpix", "Nside": 1, "ordering": 0})
        monkeypatch.setattr(z3fdb_opener, "_read_grib_grid", probe)
        return builder, probe

    return install


@pytest.mark.parametrize(
    "freq, style, clock, start, end, expected, time_keys",
    [
        (
            "D",
            "date",
            "1200",
            "20200101",
            "20200103",
            ["2020-01-01T12:00", "2020-01-02T12:00", "2020-01-03T12:00"],
            ["date", "time"],
        ),
        ("D", "date", 12, "20200101", "20200102", ["2020-01-01T12:00", "2020-01-02T12:00"], ["date", "time"]),
        ("D", "date", "0030", "20200101", "20200102", ["2020-01-01T00:30", "2020-01-02T00:30"], ["date", "time"]),
        (
            "h",
            "date",
            0,
            "20200101T2300",
            "20200102T0100",
            ["2020-01-01T23:00", "2020-01-02T00:00", "2020-01-02T01:00"],
            ["date", "time"],
        ),
        ("MS", "yearmonth", 0, "20200115", "20200315", ["2020-01-01", "2020-02-01", "2020-03-01"], ["year", "month"]),
        (
            "MS",
            "date",
            "1200",
            "20200101",
            "20200301",
            ["2020-01-01T12:00", "2020-02-01T12:00", "2020-03-01T12:00"],
            ["date", "time"],
        ),
        ("M", "yearmonth", 0, "20200101", "20200301", ["2020-01-01", "2020-02-01", "2020-03-01"], ["year", "month"]),
    ],
    ids=["daily-noon", "daily-hour-number", "daily-minutes", "hourly-midnight", "yearmonth", "monthly-date", "monthly-alias"],
)
def test_time_requests_and_labels(mock_store, freq, style, clock, start, end, expected, time_keys):
    """The exact requested timestamps and storage keys label the lazy output."""
    builder, probe = mock_store(len(expected))
    request = {"param": [130], "levtype": "sfc", "time": clock, "expver": "0001"}
    original = request.copy()
    result = z3fdb_opener.open_z3fdb(request, freq=freq, timestyle=style, startdate=start, enddate=end)
    np.testing.assert_array_equal(result.time, pd.to_datetime(expected))
    assert isinstance(result.var130.data, da.Array)
    assert request == original
    for call in builder.add_part.call_args_list:
        mars, axes, _ = call.args
        assert axes[0].keys == time_keys
        assert all(key in mars for key in time_keys)
        assert all(key not in mars for key in ({"date", "time", "year", "month"} - set(time_keys)))
    if freq in ("D", "MS") and style == "date":
        assert builder.add_part.call_args.args[0]["time"] == result.time.dt.strftime("%H%M").item(0)
    if freq == "MS" and style == "date":
        assert builder.add_part.call_args.args[0]["date"] == "20200101/20200201/20200301"
    probe.assert_called_once()
    result.attrs["history"] = "time coordinate check"
    assert result.sel(time=slice(expected[0], expected[-1])).attrs["history"] == "time coordinate check"


@pytest.mark.parametrize("clock", [{}, {"time": None}, {"time": "None"}])
@pytest.mark.parametrize("freq, end", [("D", "20200103"), ("MS", "20200301")])
def test_unspecified_date_clock_defaults_to_midnight(mock_store, clock, freq, end):
    """Legacy clock placeholders request midnight without changing the catalog request."""
    builder, _ = mock_store(3)
    request = {"param": [130], **clock}
    original = request.copy()
    result = z3fdb_opener.open_z3fdb(request, freq=freq, timestyle="date", startdate="20200101", enddate=end)
    np.testing.assert_array_equal(result.time, pd.date_range("2020-01-01", periods=3, freq=freq))
    assert request == original
    assert all(call.args[0]["time"] == "0000" for call in builder.add_part.call_args_list)


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"timestyle": "step"}, "requires data_start_date"),
        ({"timestyle": "unknown"}, "timestyle"),
        ({"timestep": "MS"}, "timestep"),
        ({"freq": "W"}, "savefreq"),
        ({"freq": "ME"}, "savefreq"),
        ({"freq": "YS-JUL"}, "savefreq"),
        ({"timestep": "6h", "freq": "h"}, "integer multiple"),
        ({"timestep": "90s"}, "whole minutes"),
        ({"timestyle": "yearmonth", "freq": "D"}, "requires monthly"),
        ({"timeshift": True}, "timeshift"),
        ({"request": {"param": [130], "time": "0000/1200"}, "freq": "D"}, "one numeric time"),
        ({"request": {"param": [130], "time": "2400"}, "freq": "D"}, "Invalid.*time"),
        ({"request": {"param": [130], "time": "noon"}, "freq": "MS", "timestyle": "date"}, "one numeric time"),
        ({"request": {"param": [130], "step": [0, 6]}}, "initial forecast step"),
    ],
)
def test_unsupported_time_settings_fail_before_fdb(mock_store, kwargs, match):
    """Unsupported settings cannot silently trigger a different FDB selection."""
    builder, probe = mock_store(1)
    options = {"request": {"param": [130]}, "freq": "h", "startdate": "20200101", "enddate": "20200101", **kwargs}
    with pytest.raises(ValueError, match=match):
        z3fdb_opener.open_z3fdb(**options)
    builder.build.assert_not_called()
    probe.assert_not_called()


def test_time_axis_size_mismatch(mock_store):
    """A shorter array cannot silently receive synthesized or truncated timestamps."""
    _, probe = mock_store(2)
    with pytest.raises(ValueError, match="3 timestamps.*2 time entries"):
        z3fdb_opener.open_z3fdb({"param": [130], "time": "1200"}, freq="D", startdate="20200101", enddate="20200103")
    probe.assert_not_called()


def test_adapter_forwards_time_settings(monkeypatch):
    """Catalog time settings reach the opener rather than remaining unused attributes."""
    opener = Mock(return_value=xr.Dataset())
    monkeypatch.setattr("aqua.core.intake_drivers.fdb.readers.open_z3fdb", opener)
    source = IntakeFDBSource(
        {"param": [130]},
        engine="z3fdb",
        data_start_date="20200101",
        data_end_date="20200301",
        savefreq="MS",
        timestyle="yearmonth",
        timestep="1h",
        timeshift=False,
    )
    source.to_dask()
    assert {key: opener.call_args.kwargs[key] for key in ("timestyle", "timestep", "timeshift")} == {
        "timestyle": "yearmonth",
        "timestep": "1h",
        "timeshift": False,
    }


@pytest.mark.parametrize("first", [False, True])
@pytest.mark.parametrize("style", ["date", "yearmonth", "step"])
def test_shared_request_formatting_preserves_gsv(style, first):
    """Extracting the shared formatter preserves GSV's three request styles."""
    source = SimpleNamespace(
        _request={"param": 130},
        _var=[130],
        chunking_vertical=None,
        timestyle=style,
        chk_start_date=[pd.Timestamp("2020-01-01")],
        chk_end_date=[pd.Timestamp("2020-03-01T12:00")],
        data_startdate="20200101",
        data_starttime="0000",
        chk_start_idx=[0],
        chk_end_idx=[1452],
        _build_time_request=FDBTimeMixin._build_time_request,
    )
    result = FDBSource._build_partition_request(source, 0, 0, first=first)
    if style == "date":
        assert result["date"] == ("20200101" if first else "20200101/to/20200301")
        assert result["time"] == ("0000" if first else "0000/to/1200")
    elif style == "yearmonth":
        assert result["year"] == "2020"
        assert result["month"] == ("01" if first else "01/to/03")
    else:
        assert result["date"] == "20200101" and result["time"] == "0000"
        assert result["step"] == ("0" if first else "0/to/1452")
    assert source._request == {"param": 130}


@pytest.mark.parametrize("style", ["date", "step"])
@pytest.mark.parametrize("timestep, savefreq", [("h", "6h"), ("6h", "6h"), ("15min", "h"), ("6h", "D")])
def test_native_and_saved_cadence(mock_store, style, timestep, savefreq):
    """Native units and saved cadence select the same timestamps as GSV."""
    start, end = "20200101T0000", "20200102T0000"
    gsv = SimpleNamespace(
        startdate=start,
        enddate=end,
        data_start_date=start,
        timeshift=False,
        bridge_end_date=None,
        _saved_time_indices=FDBTimeMixin._saved_time_indices,
    )
    FDBTimeMixin._compute_timeaxis(gsv, timestep, savefreq, "D")
    builder, _ = mock_store(len(gsv.timeaxis))
    result = z3fdb_opener.open_z3fdb(
        {"param": [130], "step": 0},
        freq=savefreq,
        timestyle=style,
        timestep=timestep,
        startdate=start,
        enddate=end,
        data_start_date=start,
    )
    np.testing.assert_array_equal(result.time, gsv.timeaxis)
    mars = builder.add_part.call_args.args[0]
    if style == "step":
        assert mars["date"] == "20200101" and mars["time"] == "0000"
        expected = np.asarray(
            (gsv.timeaxis - pd.Timestamp(start)) / pd.Timedelta(pd.tseries.frequencies.to_offset(timestep)), int
        )
        expected_step = f"{expected[0]}/to/{expected[-1]}/by/1" if timestep == savefreq else "/".join(map(str, expected))
        assert mars["step"] == expected_step
    elif savefreq == "6h":
        assert builder.add_part.call_args_list[0].args[0]["time"] == "0000/0600/1200/1800"


@pytest.mark.parametrize("initial_step", [0, 6])
def test_forecast_subwindow_keeps_original_reference(mock_store, initial_step):
    """Subwindows retain the forecast origin and honor the initial step offset."""
    builder, _ = mock_store(3 if not initial_step else 2)
    result = z3fdb_opener.open_z3fdb(
        {"param": [130], "step": initial_step},
        freq="6h",
        timestep="h",
        timestyle="step",
        startdate="20200102",
        enddate="20200102T1200",
        data_start_date="20200101T0000" if not initial_step else "20200102",
    )
    mars = builder.add_part.call_args.args[0]
    if initial_step:
        assert mars["step"] == "6/12"
        np.testing.assert_array_equal(result.time, pd.to_datetime(["2020-01-02T06:00", "2020-01-02T12:00"]))
    else:
        assert mars["date"] == "20200101" and mars["step"] == "24/30/36"
        np.testing.assert_array_equal(result.time, pd.date_range("2020-01-02", periods=3, freq="6h"))


def test_forecast_reference_across_storage_periods(mock_store):
    """HPC/bridge routing must not reset the reference date of forecast steps."""
    builder, _ = mock_store(1)
    source = IntakeFDBSource(
        {"param": [130], "step": 0},
        engine="z3fdb",
        data_start_date="20200101",
        data_end_date="20200103",
        bridge_start_date="20200102",
        bridge_end_date="20200102",
        savefreq="D",
        timestyle="step",
        timestep="h",
    )
    result = source.to_dask()
    requests = [call.args[0] for call in builder.add_part.call_args_list]
    assert [r["step"] for r in requests] == ["0", "24", "48"]
    assert all(r["date"] == "20200101" for r in requests)
    np.testing.assert_array_equal(result.time, pd.date_range("20200101", periods=3, freq="D"))


@pytest.mark.parametrize("style", ["date", "step", "yearmonth"])
def test_monthly_initial_offset(mock_store, style):
    """Monthly saved steps use the native integration units and honor the offset."""
    builder, _ = mock_store(3)
    result = z3fdb_opener.open_z3fdb(
        {"param": [130], "step": 6, "time": 0},
        freq="MS",
        timestyle=style,
        timestep="h",
        data_start_date="20200101",
        startdate="20200101",
        enddate="20200301",
    )
    expected = ["2020-01-01" if style == "yearmonth" else "2020-01-01T06:00", "2020-02-01", "2020-03-01"]
    np.testing.assert_array_equal(result.time, pd.to_datetime(expected, format="mixed"))
    if style == "step":
        assert builder.add_part.call_args.args[0]["step"] == "6/744/1440"


@pytest.mark.parametrize("style", ["date", "step", "yearmonth"])
def test_yearly_cadence(mock_store, style):
    """Year-start sampling labels yearly fields and forecast step indices."""
    builder, _ = mock_store(2)
    result = z3fdb_opener.open_z3fdb(
        {"param": [130], "step": 0},
        freq="Y",
        timestyle=style,
        startdate="20200101",
        enddate="20210101",
        data_start_date="20200101",
    )
    np.testing.assert_array_equal(result.time, pd.to_datetime(["2020-01-01", "2021-01-01"]))
    if style == "step":
        assert builder.add_part.call_args.args[0]["step"] == "0/8784"


@pytest.mark.parametrize("as_dict", [False, True])
@pytest.mark.parametrize(
    "freq, style, start, end, expected_chunks, native_size",
    [
        ("h", "date", "20200101", "20200102T2300", (24, 24), 24),
        ("h", "date", "20200101T1200", "20200103T0600", (12, 24, 7), 1),
        ("h", "step", "20200101T1200", "20200102T0600", (12, 7), 1),
        ("6h", "date", "20200101", "20200102T1800", (4, 4), 4),
        ("6h", "date", "20200101T0600", "20200102T1800", (3, 4), 1),
        ("15min", "date", "20200101T0130", "20200102T0015", (90, 2), 2),
        ("D", "date", "20200101", "20200103", (1, 1, 1), 1),
        ("MS", "yearmonth", "20200101", "20200301", (1, 1, 1), 1),
    ],
)
def test_calendar_day_chunks(mock_store, as_dict, freq, style, start, end, expected_chunks, native_size):
    """Calendar-day chunks follow timestamps, including partial days and sparse data."""
    builder, _ = mock_store(sum(expected_chunks))
    tasks = []
    with Callback(posttask=lambda *args: tasks.append(args)):
        result = z3fdb_opener.open_z3fdb(
            {"param": [130], "step": 0},
            freq=freq,
            timestyle=style,
            timestep="15min" if freq == "15min" else "h",
            startdate=start,
            enddate=end,
            data_start_date="20200101",
            chunks={"time": "D"} if as_dict else "D",
        )
    assert not tasks
    assert result.var130.chunks[0] == expected_chunks
    assert result.attrs["history"]
    if style == "step":
        assert builder.add_part.call_args.args[0]["step"] == "12/to/30/by/1"
    for call in builder.add_part.call_args_list:
        chunk = call.args[1][0].chunking
        if native_size == 1:
            assert chunk == z3fdb_opener.Chunking.SINGLE_VALUE
        else:
            assert chunk.chunkShape == native_size
    boundaries = np.cumsum(expected_chunks)[:-1]
    assert all(pd.Timestamp(result.time[index].item()).hour == 0 for index in boundaries)


@pytest.mark.parametrize("as_dict", [False, True])
@pytest.mark.parametrize(
    "chunkfreq, savefreq, start, end, bridge_start, bridge_end, sizes, expected",
    [
        ("D", "h", "20200101T0600", "20200102T1700", "20200101T1200", "20200102T0500", [6, 18, 12], (18, 18)),
        ("6h", "h", "20200101T0300", "20200101T1100", "20200101T0500", "20200101T0800", [2, 4, 3], (3, 6)),
        ("MS", "D", "20200115", "20200305", "20200125", "20200205", [10, 12, 29], (17, 29, 5)),
        ("MS", "MS", "20200101", "20200501", "20200201", "20200301", [1, 2, 2], (1, 1, 1, 1, 1)),
    ],
)
def test_time_chunks_across_storage_split(
    mock_store, as_dict, chunkfreq, savefreq, start, end, bridge_start, bridge_end, sizes, expected
):
    """Join storage fragments into lazy chunks at the requested time boundaries."""
    mock_store(sizes)
    result = IntakeFDBSource(
        {"param": [130], "time": "None"},
        engine="z3fdb",
        data_start_date=start,
        data_end_date=end,
        bridge_start_date=bridge_start,
        bridge_end_date=bridge_end,
        savefreq=savefreq,
        timestyle="date",
        chunks={"time": chunkfreq} if as_dict else chunkfreq,
    ).to_dask()
    assert result.var130.chunks[0] == expected
    np.testing.assert_array_equal(result.time, pd.date_range(pd.Timestamp(start), pd.Timestamp(end), freq=savefreq))
    assert result.attrs["history"]


@pytest.mark.parametrize("chunks", ["invalid", "0h", "-6h", {"time": "invalid"}, 0, -1, {"time": 1.5}])
def test_unsupported_chunk_frequency_is_explicit(mock_store, chunks):
    """Frequency strings must not silently become one-frame chunks."""
    builder, probe = mock_store(1)
    with pytest.raises(ValueError, match="time chunks must be"):
        z3fdb_opener.open_z3fdb(
            {"param": [130]},
            freq="h",
            startdate="20200101",
            enddate="20200101",
            chunks=chunks,
        )
    builder.build.assert_not_called()
    probe.assert_not_called()


@pytest.mark.parametrize("as_dict", [False, True])
@pytest.mark.parametrize(
    "chunkfreq, freq, style, start, end, expected, native_size",
    [
        ("6h", "h", "date", "20200101T0300", "20200102T0800", (3, 6, 6, 6, 6, 3), 3),
        ("6h", "h", "step", "20200101T0500", "20200101T1200", (1, 6, 1), 1),
        ("6h", "15min", "date", "20200101T0130", "20200101T0630", (18, 3), 3),
        ("MS", "D", "date", "20200115", "20200305", (17, 29, 5), 1),
        ("MS", "D", "step", "20200115", "20200305", (17, 29, 5), 1),
        ("MS", "h", "date", "20200131T1200", "20200301T0200", (12, 696, 3), 3),
        ("MS", "MS", "date", "20200101", "20200301", (1, 1, 1), 1),
        ("MS", "MS", "yearmonth", "20200101", "20200301", (1, 1, 1), 1),
    ],
)
def test_frequency_time_chunks(mock_store, as_dict, chunkfreq, freq, style, start, end, expected, native_size):
    """Monthly and six-hour bins honor partial windows, leap years and storage styles."""
    builder, _ = mock_store(sum(expected))
    tasks = []
    with Callback(posttask=lambda *args: tasks.append(args)):
        result = z3fdb_opener.open_z3fdb(
            {"param": [130], "time": "None"},
            freq=freq,
            timestyle=style,
            timestep="15min" if freq == "15min" else "h",
            startdate=start,
            enddate=end,
            data_start_date="20200101",
            chunks={"time": chunkfreq} if as_dict else chunkfreq,
        )
    assert not tasks
    assert result.var130.chunks[0] == expected
    assert result.attrs["history"]
    for call in builder.add_part.call_args_list:
        chunk = call.args[1][0].chunking
        if native_size == 1:
            assert chunk == z3fdb_opener.Chunking.SINGLE_VALUE
        else:
            assert chunk.chunkShape == native_size

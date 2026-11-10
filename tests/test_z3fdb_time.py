"""Time selection and chunking through the public z3fdb opener."""

import dask.array as da
import pandas as pd
import pytest
import xarray as xr
from dask.callbacks import Callback

from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.openers.z3fdb_opener import open_z3fdb

pytestmark = pytest.mark.aqua


@pytest.mark.parametrize(
    "options, clock, expected_start, count, cadence, expected_request",
    [
        ({"freq": "D"}, "1200", "20200101T1200", 3, "D", {"time": "1200"}),
        ({"freq": "D"}, 12, "20200101T1200", 3, "D", {"time": "1200"}),
        ({"freq": "D"}, "0030", "20200101T0030", 3, "D", {"time": "0030"}),
        ({"freq": "h"}, 0, "20200101T2300", 3, "h", {"date": "20200102", "time": "0000/to/0100/by/1"}),
        ({"freq": "MS", "timestyle": "yearmonth"}, 0, "20200101", 3, "MS", {"year": "2020", "month": "1/2/3"}),
        ({"freq": "M"}, 0, "20200101", 3, "MS", {"year": "2020", "month": "1/2/3"}),
        (
            {"freq": "MS", "timestyle": "date"},
            "1200",
            "20200101T1200",
            3,
            "MS",
            {"date": "20200101/20200201/20200301", "time": "1200"},
        ),
        ({"freq": "6h", "timestep": "h"}, 0, "20200101", 5, "6h", {"time": "0000"}),
        ({"freq": "6h", "timestep": "6h"}, 0, "20200101", 5, "6h", {"time": "0000"}),
        ({"freq": "h", "timestep": "15min"}, 0, "20200101", 25, "h", {"time": "0000"}),
        ({"freq": "D", "timestep": "6h"}, 0, "20200101", 2, "D", {"date": "20200101/to/20200102"}),
        ({"freq": "6h", "timestyle": "step"}, 0, "20200102", 3, "6h", {"date": "20200101", "step": "24/30/36"}),
        ({"freq": "6h", "timestep": "6h", "timestyle": "step"}, 0, "20200101", 5, "6h", {"step": "0/to/4/by/1"}),
        ({"freq": "h", "timestep": "15min", "timestyle": "step"}, 0, "20200101", 3, "h", {"step": "0/4/8"}),
        ({"freq": "D", "timestep": "6h", "timestyle": "step"}, 0, "20200101", 2, "D", {"step": "0/4"}),
        ({"freq": "Y", "timestyle": "step"}, 0, "20200101", 2, "YS", {"step": "0/8784"}),
        ({"freq": "Y", "timestyle": "date"}, 0, "20200101", 2, "YS", {"date": "20200101/20210101"}),
        ({"freq": "Y", "timestyle": "yearmonth"}, 0, "20200101", 2, "YS", {"year": "2020/2021", "month": "1"}),
    ],
)
def test_requested_times_label_output(z3fdb_store, options, clock, expected_start, count, cadence, expected_request):
    """Wrong storage keys, clocks or native step units must change the observed request or labels."""
    fdb = z3fdb_store((count, 1, 12))
    expected = pd.date_range(expected_start, periods=count, freq=cadence)
    request = {"param": [130], "time": clock}
    result = open_z3fdb(
        request,
        startdate=expected[0],
        enddate=expected[-1],
        data_start_date="20200101",
        **options,
    )
    xr.testing.assert_equal(result.time, xr.DataArray(expected, dims="time", name="time", coords={"time": expected}))
    assert expected_request.items() <= fdb.calls[-1].request.items()
    assert request == {"param": [130], "time": clock}
    assert isinstance(result.var130.data, da.Array)
    assert result.attrs["history"]


@pytest.mark.parametrize("clock", [{}, {"time": None}, {"time": "None"}])
@pytest.mark.parametrize("freq, end", [("D", "20200103"), ("MS", "20200301")])
def test_missing_clock_defaults_to_midnight(z3fdb_store, clock, freq, end):
    """Absent, null and legacy clocks must request midnight without mutating catalog input."""
    fdb = z3fdb_store((3, 1, 12))
    request = {"param": [130], **clock}
    result = open_z3fdb(request, freq=freq, timestyle="date", startdate="20200101", enddate=end)
    assert result.indexes["time"].equals(pd.date_range("20200101", periods=3, freq=freq))
    assert all(call.request["time"] == "0000" for call in fdb.calls)
    assert request == {"param": [130], **clock}


@pytest.mark.parametrize("style", ["date", "step", "yearmonth"])
def test_monthly_initial_forecast_step(z3fdb_store, style):
    """An initial step changes the first field but must not move later monthly samples."""
    fdb = z3fdb_store((3, 1, 12))
    result = open_z3fdb(
        {"param": [130], "step": 6},
        freq="MS",
        timestyle=style,
        startdate="20200101",
        enddate="20200301",
        data_start_date="20200101",
    )
    expected = pd.to_datetime(
        ["2020-01-01" if style == "yearmonth" else "2020-01-01T06:00", "2020-02-01", "2020-03-01"], format="mixed"
    )
    assert result.indexes["time"].equals(expected)
    if style == "step":
        assert fdb.calls[0].request["step"] == "6/744/1440"


@pytest.mark.parametrize(
    "chunkfreq, freq, style, start, expected, native",
    [
        ("D", "h", "date", "20200101", (24, 24), 24),
        ({"time": "D"}, "h", "date", "20200101T1200", (12, 24, 7), 1),
        ("D", "h", "step", "20200101T1200", (12, 7), 1),
        ("D", "6h", "date", "20200101", (4, 4), 4),
        ("D", "6h", "date", "20200101T0600", (3, 4), 1),
        ("D", "15min", "date", "20200101T0130", (90, 2), 2),
        ("D", "D", "date", "20200101", (1, 1, 1), 1),
        ("D", "MS", "yearmonth", "20200101", (1, 1, 1), 1),
        ("6h", "h", "date", "20200101T0300", (3, 6, 6, 6, 6, 3), 3),
        ("6h", "h", "step", "20200101T0500", (1, 6, 1), 1),
        ("6h", "15min", "date", "20200101T0130", (18, 3), 3),
        ("MS", "D", "date", "20200115", (17, 29, 5), 1),
        ({"time": "MS"}, "D", "step", "20200115", (17, 29, 5), 1),
        ("MS", "h", "date", "20200131T1200", (12, 696, 3), 3),
        ("MS", "MS", "date", "20200101", (1, 1, 1), 1),
        ("MS", "MS", "yearmonth", "20200101", (1, 1, 1), 1),
        ("6", "h", "date", "20200101", (6, 6), 6),
        ({"time": 1}, "h", "date", "20200101", (1, 1), 1),
        ({"time": 12}, "MS", "yearmonth", "20200101", (12,), 12),
    ],
)
def test_time_chunks_follow_calendar(z3fdb_store, chunkfreq, freq, style, start, expected, native):
    """Partial bins and saved cadence must produce the requested lazy chunks, including leap February."""
    fdb = z3fdb_store((sum(expected), 1, 12))
    times = pd.date_range(start, periods=sum(expected), freq=freq)
    tasks = []
    with Callback(posttask=lambda *args: tasks.append(args)):
        result = open_z3fdb(
            {"param": [130]},
            freq=freq,
            timestyle=style,
            timestep="15min" if freq == "15min" else "h",
            startdate=start,
            enddate=times[-1],
            data_start_date="20200101",
            chunks=chunkfreq,
        )
    assert not tasks
    assert result.var130.chunksizes["time"] == expected
    assert result.indexes["time"].equals(times)
    assert all(getattr(call.axes[0].chunking, "chunkShape", 1) == native for call in fdb.calls)
    assert result.attrs["history"]


@pytest.mark.parametrize(
    "chunks, freq, start, end, bridge_start, bridge_end, counts, expected",
    [
        ("D", "h", "20200101T0600", "20200102T1700", "20200101T1200", "20200102T0500", [6, 18, 12], (18, 18)),
        ("6h", "h", "20200101T0300", "20200101T1100", "20200101T0500", "20200101T0800", [2, 4, 3], (3, 6)),
        ({"time": "MS"}, "D", "20200115", "20200305", "20200125", "20200205", [10, 12, 29], (17, 29, 5)),
        ("MS", "MS", "20200101", "20200501", "20200201", "20200301", [1, 2, 2], (1, 1, 1, 1, 1)),
    ],
)
def test_chunks_join_storage_periods(z3fdb_store, chunks, freq, start, end, bridge_start, bridge_end, counts, expected):
    """Storage switches inside a calendar bin must not split the output chunk or lose timestamps."""
    z3fdb_store([(count, 1, 12) for count in counts])
    result = IntakeFDBSource(
        {"param": [130], "time": "None"},
        engine="z3fdb",
        savefreq=freq,
        chunks=chunks,
        data_start_date=start,
        data_end_date=end,
        bridge_start_date=bridge_start,
        bridge_end_date=bridge_end,
    ).to_dask()
    assert result.var130.chunksizes["time"] == expected
    assert result.indexes["time"].equals(pd.date_range(start, end, freq=freq))
    assert result.attrs["history"]


@pytest.mark.parametrize(
    "options, match",
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
        *[
            ({"chunks": chunks}, "time chunks must be")
            for chunks in ("invalid", "0h", "-6h", {"time": "invalid"}, 0, -1, {"time": 1.5})
        ],
    ],
)
def test_invalid_settings_stop_before_fdb(z3fdb_store, options, match):
    """Invalid time or chunk settings must fail rather than retrieve a different selection."""
    fdb = z3fdb_store((1, 1, 12))
    with pytest.raises(ValueError, match=match):
        open_z3fdb(**dict(request={"param": [130]}, freq="h", startdate="20200101", enddate="20200101") | options)
    assert not fdb.calls
    fdb.retrieve.assert_not_called()


def test_missing_native_frame_rejected(z3fdb_store):
    """A shorter native array cannot silently receive synthesized or truncated dates."""
    z3fdb_store((2, 1, 12))
    with pytest.raises(ValueError, match="3 timestamps.*2 time entries"):
        open_z3fdb({"param": [130], "time": "1200"}, freq="D", startdate="20200101", enddate="20200103")


@pytest.mark.parametrize(
    "options, count, start, cadence, dates",
    [
        ({"freq": "D", "request": {"param": [130], "year": 2020}}, 366, "20200101", "D", ["20200101/to/20201231"]),
        ({"freq": "D", "request": {"param": [130], "year": [2020, 2021]}}, 731, "20200101", "D", ["20200101/to/20211231"]),
        (
            {"freq": "h", "startdate": "20200101T1200", "enddate": "20200103T1200"},
            49,
            "20200101T1200",
            "h",
            ["20200101", "20200102", "20200103"],
        ),
        (
            {"freq": "h", "startdate": "20200101T1200", "enddate": "20200104T1200"},
            73,
            "20200101T1200",
            "h",
            ["20200101", "20200102/to/20200103", "20200104"],
        ),
        ({"freq": "MS", "startdate": "20200101", "enddate": "20211201"}, 24, "20200101", "MS", ["2020/2021"]),
        ({"freq": "MS", "startdate": "20200101", "enddate": "20210201"}, 14, "20200101", "MS", ["2020", "2021"]),
    ],
)
def test_request_windows(z3fdb_store, options, count, start, cadence, dates):
    """Full years and partial days must select every timestamp and preserve year/month request grouping."""
    fdb = z3fdb_store((count, 1, 12))
    result = open_z3fdb(**({"request": {"param": [130]}} | options))
    assert result.indexes["time"].equals(pd.date_range(start, periods=count, freq=cadence))
    assert [call.request["year" if cadence == "MS" else "date"] for call in fdb.calls] == dates


@pytest.mark.parametrize("bound", ["startdate", "enddate"])
def test_incomplete_window_rejected(z3fdb_store, bound):
    """One explicit boundary cannot silently become a full-year request."""
    fdb = z3fdb_store((1, 1, 12))
    with pytest.raises(ValueError, match="provide both start_date and end_date"):
        open_z3fdb({"param": [130], "year": 2020}, freq="D", **{bound: "20200101"})
    assert not fdb.calls


@pytest.mark.parametrize(
    "origin, initial_step, first, steps",
    [
        ("20200101", 0, "20200101", ["0/24", "48", "72"]),
        ("20200101T0600", 6, "20200101T1200", ["12/24", "48", "72"]),
    ],
)
def test_forecast_origin_survives_storage_split(z3fdb_store, origin, initial_step, first, steps):
    """Routing and date flooring must not reset the forecast reference or discard its raw initial offset."""
    fdb = z3fdb_store([(2, 1, 12), (1, 1, 12), (1, 1, 12)])
    result = IntakeFDBSource(
        {"param": [130], "step": initial_step},
        engine="z3fdb",
        savefreq="D",
        timestyle="step",
        data_start_date=origin,
        data_end_date="20200104",
        bridge_start_date="20200103",
        bridge_end_date="20200103",
    ).to_dask()
    assert [call.request["step"] for call in fdb.calls] == steps
    assert all(call.request["date"] == "20200101" for call in fdb.calls)
    assert result.indexes["time"].equals(pd.to_datetime([first, "20200102", "20200103", "20200104"], format="mixed"))

"""Tests for GSV timeutile in AQUA"""

import pandas as pd
import pytest
import xarray as xr

from aqua.core.intake_drivers.fdb.openers import gsv_source, polytope_source
from aqua.core.intake_drivers.fdb.openers.timeutil import FDBTimeMixin

floor_datetime = FDBTimeMixin._floor_datetime

# Assuming floor_datetime is already imported


@pytest.mark.aqua
def test_floor_datetime_all_cases():
    test_cases = [
        # Valid frequencies and expected results
        ("2025-01-28 15:30:45", "h", "20250128T1500"),  # Hour
        ("2025-01-28 15:30:45", "H", ValueError),  # Old hour value error
        ("2025-01-28 15:30:45", "D", "20250128T0000"),  # Day
        ("2025-03-15 15:30:45", "M", "20250301T0000"),  # Month
        ("2025-03-15 15:30:45", "Y", "20250101T0000"),  # Year
        ("2025-03-15 12:45:00", "MS", "20250301T0000"),  # Month Start
        ("2025-03-15 12:45:00", "YS", "20250101T0000"),  # Year Start
        ("2025-01-28 15:30:45", "min", "20250128T1530"),  # Minute
        ("2025-01-28 15:30:45", "T", ValueError),  # old minute value error
        ("2025-01-28 15:30:45", "s", "20250128T1530"),  # Second
        ("2025-01-28 15:30:45", "2W", KeyError),  # Week (start of the week)
        ("2025-01-28 15:30:45", "3h", "20250128T1500"),  # Custom frequency: every 3 hours
        ("2025-01-28 15:30:45", "1D", "20250128T0000"),  # Custom frequency: daily
        ("2025-01-28 15:30:45", "ME", KeyError),  # Invalid frequency should raise a KeyError
        ("2025-01-28 15:30:45", "YE", KeyError),  # Invalid frequency should raise a KeyError
    ]

    for dt_str, freq, expected in test_cases:
        dt = pd.Timestamp(dt_str)
        if isinstance(expected, str):  # For valid cases
            result = floor_datetime(dt, freq)
            assert result == expected, f"Failed for {freq} on {dt_str}: expected {expected}, got {result}"
        elif expected is KeyError:  # For invalid frequencies
            with pytest.raises(KeyError):
                floor_datetime(dt, freq)
        elif expected is ValueError:  # For invalid frequencies
            with pytest.raises(ValueError):
                floor_datetime(dt, freq)


@pytest.fixture(params=[gsv_source, polytope_source], ids=["gsv", "polytope"])
def fdb_source_factory(request, monkeypatch, tmp_path):
    """Construct real sources with only the optional FDB boundary replaced."""
    module = request.param
    monkeypatch.setattr(module, "gsv_available", True)
    monkeypatch.setenv("FDB_HOME", str(tmp_path))
    source_class = module.GSVSource if module is gsv_source else module.PolytopeSource

    def make_source(**kwargs):
        return source_class(
            request={"param": 167},
            data_start_date="20240101",
            data_end_date="20251231",
            metadata={"fdb_home": str(tmp_path)},
            **kwargs,
        )

    return module, make_source


@pytest.mark.aqua
@pytest.mark.parametrize(
    "chunks, sizes, starts, ends",
    [
        (12, [12, 12, 3], [5, 17, 29], [16, 28, 31]),
        ({"time": 12}, [12, 12, 3], [5, 17, 29], [16, 28, 31]),
        ({"time": 100}, [27], [5], [31]),
        ({"time": 1}, [1] * 27, list(range(5, 32)), list(range(5, 32))),
        ("s", [1] * 27, list(range(5, 32)), list(range(5, 32))),
        ("12h", [7, 12, 8], [5, 12, 24], [11, 23, 31]),
    ],
)
def test_time_chunks_count_frames_from_requested_start(fdb_source_factory, chunks, sizes, starts, ends):
    """Counts must include a partial final chunk without changing frequency-string alignment."""
    _, make_source = fdb_source_factory
    source = make_source(chunks=chunks, startdate="20240101T0500", enddate="20240102T0700", timestyle="step")
    assert source.chk_size.tolist() == sizes
    assert source.chk_start_idx.tolist() == starts
    assert source.chk_end_idx.tolist() == ends
    assert len(source.timeaxis) == 27


@pytest.mark.aqua
@pytest.mark.parametrize(
    "savefreq, timestep, start, end, timestyle, expected_starts, expected_ends",
    [
        ("6h", "h", "20240101T0600", "20240104T0600", "step", [6, 78], [72, 78]),
        ("MS", "MS", "20241101", "20251101", "yearmonth", [10, 22], [21, 22]),
        ("MS", "h", "20241101", "20251101", "step", [7320, 16080], [15336, 16080]),
    ],
)
def test_integer_chunks_count_saved_frames_not_native_steps(
    fdb_source_factory, savefreq, timestep, start, end, timestyle, expected_starts, expected_ends
):
    """Twelve frames must span 72 hours for six-hourly data and twelve calendar months for monthly data."""
    _, make_source = fdb_source_factory
    source = make_source(
        chunks={"time": 12}, savefreq=savefreq, timestep=timestep, startdate=start, enddate=end, timestyle=timestyle
    )
    assert source.chk_size.tolist() == [12, 1]
    assert source.chk_start_idx.tolist() == expected_starts
    assert source.chk_end_idx.tolist() == expected_ends
    assert len(source.timeaxis) == 13


@pytest.mark.aqua
@pytest.mark.parametrize("chunks", [0, -1, True, False])
def test_integer_time_chunks_reject_invalid_counts(fdb_source_factory, chunks):
    """Invalid counts must raise a clear error instead of failing in upper() or grouping."""
    _, make_source = fdb_source_factory
    with pytest.raises(ValueError, match="Time chunk size must be a positive integer"):
        make_source(chunks={"time": chunks}, startdate="20240101", enddate="20240102")


@pytest.mark.aqua
@pytest.mark.parametrize("chunks", [12, 13])
def test_integer_chunks_preserve_bridge_alignment_checks(fdb_source_factory, chunks):
    """A chunk must not silently retrieve frames from two different FDB stores."""
    _, make_source = fdb_source_factory
    kwargs = {
        "chunks": chunks,
        "startdate": "20240101",
        "enddate": "20240101T2300",
        "bridge_start_date": "20240101T1200",
        "bridge_end_date": "20240101T2300",
    }
    if chunks == 13:
        with pytest.raises(ValueError, match="Chunk size is not aligned"):
            make_source(**kwargs)
    else:
        source = make_source(**kwargs)
        assert source.chk_size.tolist() == [12, 12]
        assert source.chk_type.tolist() == [0, 1]


@pytest.mark.aqua
@pytest.mark.parametrize("method", ["read", "to_dask"])
@pytest.mark.parametrize(
    "freq, start, end, timestyle",
    [("h", "20240101T0500", "20240102T0700", "date"), ("MS", "20241101", "20251101", "yearmonth")],
)
def test_integer_chunks_read_across_calendar_boundaries(fdb_source_factory, monkeypatch, freq, start, end, timestyle, method):
    """Cartesian FDB selectors must neither lose frames nor return frames outside each chunk."""
    module, make_source = fdb_source_factory
    dates = pd.date_range("20240101", "20251231", freq=freq)
    data = xr.Dataset({"2t": ("time", range(len(dates)), {"GRIB_paramId": 167})}, coords={"time": dates})

    def selected(value):
        if isinstance(value, list):
            return [int(item) for item in value]
        bounds = str(value).split("/to/")
        return range(int(bounds[0]), int(bounds[-1]) + 1)

    class Retriever:
        def __init__(self, **kwargs):
            pass

        def request_data(self, request, **kwargs):
            time = data.time.dt
            if "year" in request:
                mask = time.year.isin(selected(request["year"])) & time.month.isin(selected(request["month"]))
            else:
                mask = time.strftime("%Y%m%d").astype(int).isin(selected(request["date"]))
                mask &= time.strftime("%H%M").astype(int).isin(selected(request["time"]))
            return data.sel(time=mask)

    monkeypatch.setattr(module, "GSVRetriever", Retriever, raising=False)
    source = make_source(chunks={"time": 12}, startdate=start, enddate=end, savefreq=freq, timestep=freq, timestyle=timestyle)
    result = getattr(source, method)()
    if method == "to_dask":
        assert result["2t"].chunksizes["time"] == ((12, 12, 3) if freq == "h" else (12, 1))
    xr.testing.assert_equal(result, data.sel(time=slice(pd.Timestamp(start), pd.Timestamp(end))))

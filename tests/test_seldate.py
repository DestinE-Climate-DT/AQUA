import numpy as np
import pandas as pd
import pytest
import xarray as xr

from aqua.core.backend.backend import Backend
from aqua.core.reader.reader import Reader


class DummyBackend(Backend):
    """Dummy backend implementing abstract methods for testing."""

    def retrieve(self, *args, **kwargs):
        pass

    def retrieve_plain(self, *args, **kwargs):
        pass

    def log_history(self, data, *args, **kwargs):
        return data


@pytest.fixture
def sample_dataset():
    """Create a sample dataset spanning several days with sub-daily timesteps."""
    times = pd.date_range("1985-01-01", "1985-01-05 18:00:00", freq="6h")
    data = np.arange(len(times), dtype=float)
    return xr.Dataset({"var": ("time", data)}, coords={"time": times})


@pytest.mark.aqua
class TestSeldate:
    """Test seldate functionality in Backend and Reader."""

    def test_backend_seldate_single_date(self, sample_dataset):
        backend = DummyBackend()
        # Single date selection (e.g., '1985-01-01')
        res = backend.seldate(sample_dataset, "1985-01-01")
        assert len(res.time) == 4  # 00:00, 06:00, 12:00, 18:00
        assert pd.Timestamp(res.time.values[0]) == pd.Timestamp("1985-01-01 00:00:00")
        assert pd.Timestamp(res.time.values[-1]) == pd.Timestamp("1985-01-01 18:00:00")

    def test_backend_seldate_single_date_keyword(self, sample_dataset):
        backend = DummyBackend()
        res = backend.seldate(sample_dataset, startdate="1985-01-02")
        assert len(res.time) == 4
        assert pd.Timestamp(res.time.values[0]) == pd.Timestamp("1985-01-02 00:00:00")
        assert pd.Timestamp(res.time.values[-1]) == pd.Timestamp("1985-01-02 18:00:00")

    def test_backend_seldate_range(self, sample_dataset):
        backend = DummyBackend()
        res = backend.seldate(sample_dataset, "1985-01-01", "1985-01-02")
        assert len(res.time) == 8  # 4 on Jan 1 + 4 on Jan 2
        assert pd.Timestamp(res.time.values[0]) == pd.Timestamp("1985-01-01 00:00:00")
        assert pd.Timestamp(res.time.values[-1]) == pd.Timestamp("1985-01-02 18:00:00")

    def test_backend_seldate_open_ended(self, sample_dataset):
        backend = DummyBackend()
        # startdate provided, enddate explicitly None -> all remaining data
        res = backend.seldate(sample_dataset, "1985-01-04", enddate=None)
        assert len(res.time) == 8  # Jan 4 (4) + Jan 5 (4)
        assert pd.Timestamp(res.time.values[0]) == pd.Timestamp("1985-01-04 00:00:00")
        assert pd.Timestamp(res.time.values[-1]) == pd.Timestamp("1985-01-05 18:00:00")

    def test_backend_seldate_start_none(self, sample_dataset):
        backend = DummyBackend()
        # startdate None, enddate provided -> all data up to enddate
        res = backend.seldate(sample_dataset, startdate=None, enddate="1985-01-02")
        assert len(res.time) == 8
        assert pd.Timestamp(res.time.values[0]) == pd.Timestamp("1985-01-01 00:00:00")
        assert pd.Timestamp(res.time.values[-1]) == pd.Timestamp("1985-01-02 18:00:00")

    def test_backend_seldate_both_none(self, sample_dataset):
        backend = DummyBackend()
        res = backend.seldate(sample_dataset, None, None)
        assert len(res.time) == len(sample_dataset.time)

    def test_reader_seldate_wrapper(self, sample_dataset):
        # Create a Reader with dummy backend
        reader = object.__new__(Reader)
        reader.backend = DummyBackend()

        # Test single date positional
        res1 = reader.seldate(sample_dataset, "1985-01-01")
        assert len(res1.time) == 4

        # Test single date keyword
        res2 = reader.seldate(sample_dataset, startdate="1985-01-01")
        assert len(res2.time) == 4

        # Test range
        res3 = reader.seldate(sample_dataset, "1985-01-01", "1985-01-02")
        assert len(res3.time) == 8

        # Test open ended with enddate=None
        res4 = reader.seldate(sample_dataset, "1985-01-04", enddate=None)
        assert len(res4.time) == 8

    def test_accessor_seldate(self, sample_dataset):
        # Import accessor to register .aqua
        import aqua.core.accessor  # noqa: F401

        reader = object.__new__(Reader)
        reader.backend = DummyBackend()
        reader.set_default = lambda: None
        sample_dataset.aqua.set_default(reader)

        # Single date via accessor
        res = sample_dataset.aqua.seldate("1985-01-01")
        assert len(res.time) == 4

        # Range via accessor
        res_range = sample_dataset.aqua.seldate("1985-01-01", "1985-01-02")
        assert len(res_range.time) == 8

        # Open-ended via accessor
        res_open = sample_dataset.aqua.seldate("1985-01-04", enddate=None)
        assert len(res_open.time) == 8

    def test_backend_seldate_datetime_objects(self, sample_dataset):
        import datetime

        backend = DummyBackend()

        # Test datetime.date / datetime.datetime objects
        dt_start = datetime.datetime(1985, 1, 1, 0, 0)
        dt_end = datetime.datetime(1985, 1, 2, 12, 0)
        res = backend.seldate(sample_dataset, dt_start, dt_end)
        assert len(res.time) == 7  # 00:00, 06:00, 12:00, 18:00 (Jan 1) + 00:00, 06:00, 12:00 (Jan 2)

        # Test pd.Timestamp objects
        ts_start = pd.Timestamp("1985-01-01 06:00:00")
        ts_end = pd.Timestamp("1985-01-01 18:00:00")
        res_ts = backend.seldate(sample_dataset, ts_start, ts_end)
        assert len(res_ts.time) == 3
        assert pd.Timestamp(res_ts.time.values[0]) == ts_start
        assert pd.Timestamp(res_ts.time.values[-1]) == ts_end

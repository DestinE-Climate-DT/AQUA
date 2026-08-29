"""Test cases for the Trender class."""

import numpy as np
import pandas as pd
import pytest
import xarray as xr
from conftest import LOGLEVEL

from aqua import Reader
from aqua.core.reader.trender import dynamic_inferred_freq

loglevel = LOGLEVEL


@pytest.mark.aqua
class TestTrender:
    """Test class for Trender functionality."""

    @pytest.fixture(scope="class")
    def reader(self):
        return Reader(model="IFS", exp="test-tco79", source="long", loglevel=loglevel)

    @pytest.fixture(scope="class")
    def data(self, reader):
        """Retrieve all data once for all tests in this class"""
        return reader.retrieve()

    def test_coeffs_dataset(self, reader, data):
        """Test for polynomial coefficients on Dataset"""
        block1 = data.isel(time=slice(0, 1000))
        coeffs = reader.trender.coeffs(block1, degree=1)
        avg = coeffs["2t"].sel(degree=1).mean().values
        assert float(avg) == pytest.approx(-7.91903731e-17, rel=1e-5)
        coeffs = reader.trender.coeffs(block1, degree=1, normalize=True)
        avg = coeffs["2t"].sel(degree=1).mean().values
        assert float(avg) == pytest.approx(-0.0002850853431, rel=1e-5)

    def test_coeffs_monthly_irregular(self, reader):
        """Test for polynomial coefficients normalization on monthly data with slightly irregular intervals."""
        # Monthly data centered around mid-month (e.g. 15th and 16th)
        times = pd.to_datetime(
            [
                "2020-01-15",
                "2020-02-15",
                "2020-03-16",
                "2020-04-15",
                "2020-05-16",
                "2020-06-15",
                "2020-07-15",
                "2020-08-16",
                "2020-09-15",
                "2020-10-16",
                "2020-11-15",
                "2020-12-15",
            ]
        )
        da = xr.DataArray(
            np.arange(len(times), dtype=float),
            coords={"time": times},
            dims=["time"],
            name="monthly_var",
        )
        coeffs = reader.trender.coeffs(da, degree=1, normalize=True)
        assert coeffs is not None
        # Degree 1 coefficient should be close to 1.0 (1 unit increase per month)
        slope = coeffs.sel(degree=1).values
        assert float(slope) == pytest.approx(1.0, rel=0.05)

    def test_trend_dataarray(self, reader, data):
        """Trivial test for trend on DataArray"""
        block1 = data["2t"].isel(time=slice(0, 1000))
        trend1 = reader.trender.trend(block1).aqua.fldmean()

        assert trend1.shape == (1000,)
        assert pytest.approx(trend1.values[300]) == 285.908

    def test_detrend_dataarray(self, reader, data):
        """Trivial test for detrending on DataArray"""
        block1 = data["2t"].isel(time=slice(0, 1000))
        det1 = reader.detrend(block1).aqua.fldmean()

        assert det1.shape == (1000,)
        assert pytest.approx(det1.values[300]) == 0.3778275

    def test_detrend_dataset(self, reader, data):
        """Second trivial test for detrending on Dataset"""
        block2 = data[["2t", "skt"]].isel(time=slice(0, 100))
        det2 = reader.detrend(block2, dim="time", degree=2)

        assert list(det2.data_vars) == ["2t", "skt"]
        assert pytest.approx(det2["skt"].isel(time=10, lon=2, lat=2).values) == -0.098381225331

    @pytest.mark.parametrize(
        "freq_str, expected",
        [
            ("YS", "YS"),
            ("MS", "MS"),
            ("W", "W"),
            ("D", "D"),
            ("6h", "6h"),
            ("h", "h"),
            ("30min", "30min"),
            ("min", "min"),
        ],
    )
    def test_dynamic_inferred_freq(self, freq_str, expected):
        """Test dynamic_inferred_freq with various standard frequencies."""
        dates = pd.date_range("2020-01-01", periods=5, freq=freq_str)
        da = xr.DataArray(np.arange(len(dates)), coords={"time": dates}, dims=["time"])
        assert dynamic_inferred_freq(da["time"]) == expected
        assert dynamic_inferred_freq(dates) == expected

    def test_dynamic_inferred_freq_edge_cases(self):
        """Test dynamic_inferred_freq with edge cases."""
        single_date = pd.DatetimeIndex(["2020-01-01"])
        assert dynamic_inferred_freq(single_date) is None
        empty_date = pd.DatetimeIndex([])
        assert dynamic_inferred_freq(empty_date) is None

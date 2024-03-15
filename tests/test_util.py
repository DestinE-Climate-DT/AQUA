"""Test for timmean method"""

import pytest
import datetime
import xarray as xr
import numpy as np
from aqua.util import extract_literal_and_numeric, file_is_complete

@pytest.fixture
def test_text():
    return [
        ("1D", ("D", 1)),
        ("MS", ("MS", 1)),
        ("300h", ("h", 300)),
        ("", (None, None)),
    ]

@pytest.mark.aqua
def test_extract_literal_and_numeric(test_text):
    for input_text, expected_output in test_text:
        result = extract_literal_and_numeric(input_text)
        assert result == expected_output

# Define a fixture to create a sample netCDF file for testing
@pytest.mark.aqua
class TestFileIsComplete:
    """The File is Complete testing class"""


    @pytest.fixture
    def sample_netcdf(self, tmp_path):
        """Create a sample Dataset and its file"""
        filename = tmp_path / "sample_netcdf.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        time_values = [datetime.datetime(2024, 1, 1) + datetime.timedelta(days=i) for i in range(3)]
        data = data.assign_coords(time=time_values)
        data.name = "sample_data"
        dataset = xr.Dataset({"sample_data": data})
        dataset.to_netcdf(filename)
        return filename
    
    def test_file_is_complete_existing_file(self, sample_netcdf):
        result = file_is_complete(sample_netcdf)
        assert result is True

    @pytest.mark.parametrize("mindate,expected_result", [("2023-12-31", False), ("2025-01-01", True)])
    def test_file_is_complete_full_nan_with_mindate(self, tmp_path, mindate, expected_result):
        filename = tmp_path / "sample_netcdf.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        time_values = [datetime.datetime(2024, 1, 1) + datetime.timedelta(days=i) for i in range(3)]
        data = data.assign_coords(time=time_values)
        data.name = "sample_data"
        data[:,:,:] = np.nan
        dataset = xr.Dataset({"sample_data": data})
        dataset["sample_data"].attrs["mindate"] = mindate
        dataset.to_netcdf(filename)
        result = file_is_complete(filename, loglevel='info')
        assert result == expected_result
    
    @pytest.mark.parametrize("mindate,expected_result", [("2023-12-31", False), ("2024-02-01", True)])
    def test_file_is_complete_partial_nan_with_mindate(self, tmp_path, mindate, expected_result):
        filename = tmp_path / "sample_netcdf.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        time_values = [datetime.datetime(2024, 1, 1) + datetime.timedelta(days=i*40) for i in range(3)]
        data = data.assign_coords(time=time_values)
        data.name = "sample_data"
        data[0,:,:] = np.nan
        dataset = xr.Dataset({"sample_data": data})
        dataset["sample_data"].attrs["mindate"] = mindate
        dataset.to_netcdf(filename)
        result = file_is_complete(filename, loglevel='info')
        assert result == expected_result

    def test_file_is_complete_nonexistent_file(self, tmp_path):
        non_existent_file = tmp_path / "non_existent.nc"
        result = file_is_complete(non_existent_file)
        assert result is False

    def test_file_is_complete_empty_file(self, tmp_path):
        empty_file = tmp_path / "empty.nc"
        xr.DataArray().to_netcdf(empty_file)
        result = file_is_complete(empty_file)
        assert result is False

    def test_file_is_complete_nan_file(self, tmp_path):
        nan_file = tmp_path / "nan.nc"
        nan_data = xr.DataArray(np.full((3, 4, 5), np.nan), dims=("time", "lat", "lon"))
        nan_data.to_netcdf(nan_file)
        result = file_is_complete(nan_file)
        assert result is False
        #assert "full of NaN" in caplog.text

    def test_file_is_complete_with_missing_time(self, tmp_path):
        valid_with_missing_time_file = tmp_path / "valid_with_missing_time.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        data[0,:,:] = np.nan # Introduce NaN value
        data.to_netcdf(valid_with_missing_time_file)
        result = file_is_complete(valid_with_missing_time_file)
        assert result is False

    def test_file_is_complete_valid_with_nan(self, tmp_path):
        valid_with_nan_file = tmp_path / "valid_with_nan.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        data[:,1,:] = np.nan # Introduce NaN value
        data.to_netcdf(valid_with_nan_file)
        result = file_is_complete(valid_with_nan_file)
        assert result is True



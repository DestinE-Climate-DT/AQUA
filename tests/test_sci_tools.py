import pytest

import xarray as xr
import numpy as np
from aqua.util import area_selection, select_season

loglevel = 'DEBUG'

@pytest.fixture
def sample_data():
    """Create a sample DataArray for testing"""
    data = xr.DataArray(
        np.random.rand(6, 6),
        coords={'lat': [10, 20, 30, 40, 50, 60], 'lon': [40, 50, 60, 70, 80, 90]},
        dims=['lat', 'lon']
    )
    return data

@pytest.mark.aqua
def test_valid_selection_no_brd(sample_data):
    """Test with valid latitude and longitude ranges, no box_brd"""
    lat_range = [10, 30]
    lon_range = [45, 55]
    result = area_selection(sample_data, lat=lat_range, lon=lon_range,
                            box_brd=False, loglevel=loglevel)

    assert result is not None
    assert np.isnan(result.sel(lat=10, lon=40).values)
    assert np.isnan(result.sel(lat=60, lon=90).values)


@pytest.mark.aqua
def test_valid_selection(sample_data):
    """Test with valid latitude and longitude ranges"""
    lat_range = [15, 25]
    lon_range = [45, 55]
    result = area_selection(sample_data, lat=lat_range, lon=lon_range,
                            box_brd=True, loglevel=loglevel)

    assert result is not None
    assert np.isnan(result.sel(lat=10, lon=40).values)
    assert np.isnan(result.sel(lat=60, lon=90).values)


@pytest.mark.aqua
def test_missing_lat_lon(sample_data):
    """Test when both lat and lon are None, should raise a ValueError"""
    with pytest.raises(ValueError):
        area_selection(sample_data, lat=None, lon=None, box_brd=True,
                       loglevel=loglevel)


@pytest.mark.aqua
def test_missing_lat_lon_coords():
    """Test with missing lat or lon coordinates, should raise an KeyError"""
    data_missing_lat = xr.DataArray(np.random.rand(3, 3),
                                    coords={'lon': [40, 50, 60]},
                                    dims=['lat', 'lon'])
    data_missing_lon = xr.DataArray(np.random.rand(3, 3),
                                    coords={'lat': [10, 20, 30]},
                                    dims=['lat', 'lon'])

    with pytest.raises(KeyError):
        area_selection(data_missing_lat, lat=[15, 25], lon=[45, 55],
                       box_brd=True, loglevel=loglevel)

    with pytest.raises(KeyError):
        area_selection(data_missing_lon, lat=[15, 25], lon=[45, 55],
                       box_brd=True, loglevel=loglevel)


@pytest.mark.aqua
def test_missing_data():
    """Test with missing data, should raise a ValueError"""
    with pytest.raises(ValueError):
        area_selection(None, lat=[15, 25], lon=[45, 55],
                       box_brd=True, loglevel=loglevel)


@pytest.mark.aqua
def test_select_season():
    """Test the select_season function with valid and invalid inputs."""
    # Sample DataArray with a time dimension
    times = xr.date_range(start="2000-01-01", periods=12, freq="MS")
    data = xr.DataArray(np.random.rand(12), coords={"time": times}, dims=["time"])

    # Valid season tests
    for season, expected_months in {"DJF": [12, 1, 2], "MAM": [3, 4, 5]}.items():
        result = select_season(data, season)
        assert len(result) == 3
        assert all(month in expected_months for month in result["time.month"].values)

    # Invalid season test
    with pytest.raises(ValueError):
        select_season(data, "XYZ")

    # Missing time dimension test
    data_no_time = xr.DataArray(np.random.rand(12), dims=["dim_0"])
    with pytest.raises(KeyError):
        select_season(data_no_time, "DJF")

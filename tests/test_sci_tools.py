import pytest

import xarray as xr
import numpy as np
from aqua.util import area_selection

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

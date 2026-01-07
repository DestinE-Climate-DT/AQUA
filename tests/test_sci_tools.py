import pytest
import regionmask
import xarray as xr
import numpy as np
from typeguard import TypeCheckError
from aqua.core.fldstat import AreaSelection
from aqua.core.util import select_season
from aqua.core.util.sci_util import generate_quarter_months
from aqua.core.util import check_seasonal_chunk_completeness

from conftest import LOGLEVEL

loglevel = LOGLEVEL

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
    result = AreaSelection(loglevel=loglevel).select_area(sample_data,
                                                          lat=lat_range, lon=lon_range,
                                                          box_brd=False)

    assert result is not None
    assert np.isnan(result.sel(lat=10, lon=40).values)
    assert np.isnan(result.sel(lat=60, lon=90).values)


@pytest.mark.aqua
def test_selection_regionmask(sample_data):
    """Test selection using regionmask"""
    # Define a simple regionmask region
    region = regionmask.defined_regions.natural_earth_v5_0_0.countries_110
    region_sel = ['United States of America', 'Russia']

    result = AreaSelection(loglevel=loglevel).select_area(sample_data,
                                                          region=region,
                                                          region_sel=region_sel,
                                                          box_brd=True)

    assert result is not None
    # Check that values outside the selected regions are NaN
    assert np.isnan(result.sel(lat=0, lon=150, method="nearest").values)
    # Check that values inside the selected regions are not NaN
    assert result.sel(lat=40, lon=-100, method="nearest").values is not np.nan


@pytest.mark.aqua
def test_valid_selection(sample_data):
    """Test with valid latitude and longitude ranges"""
    lat_range = [15, 25]
    lon_range = [45, 55]
    result = AreaSelection(loglevel=loglevel).select_area(sample_data,
                                                          lat=lat_range, lon=lon_range,
                                                          box_brd=True)

    assert result is not None
    assert np.isnan(result.sel(lat=10, lon=40).values)
    assert np.isnan(result.sel(lat=60, lon=90).values)


@pytest.mark.aqua
def test_missing_lat_lon(sample_data):
    """Test when both lat and lon are None, the sample_data is returned"""
    result = AreaSelection(loglevel=loglevel).select_area(sample_data, lat=None, lon=None, box_brd=True)
    assert result is not None
    assert result.equals(sample_data)


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
        AreaSelection(loglevel=loglevel).select_area(data_missing_lat, lat=[15, 25], lon=[45, 55],
                                                      box_brd=True)

    with pytest.raises(KeyError):
        AreaSelection(loglevel=loglevel).select_area(data_missing_lon, lat=[15, 25], lon=[45, 55],
                                                      box_brd=True)


@pytest.mark.aqua
def test_missing_data():
    """Test with missing data or wrong type"""
    with pytest.raises(TypeError):
        AreaSelection(loglevel=loglevel).select_area(lat=[15, 25], lon=[45, 55],
                                                      box_brd=True)
        
    with pytest.raises(TypeCheckError):
        AreaSelection(loglevel=loglevel).select_area('invalid_data', lat=[15, 25], lon=[45, 55],
                                                      box_brd=True)


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


@pytest.mark.aqua
def test_generate_quarter_months():
    """Test the generate_quarter_months function with various anchor months."""
    result = generate_quarter_months('DEC')
    assert result == {'DEC': {'Q1': [12, 1, 2], 'Q2': [3, 4, 5], 'Q3': [6, 7, 8], 'Q4': [9, 10, 11]}}

    result = generate_quarter_months('MAR')
    assert result == {'MAR': {'Q1': [3, 4, 5], 'Q2': [6, 7, 8], 'Q3': [9, 10, 11], 'Q4': [12, 1, 2]}}
    with pytest.raises(ValueError):
        generate_quarter_months('XXX')


@pytest.mark.aqua
def test_check_seasonal_chunk_completeness():
    # Daily data:
    t_dec = xr.date_range("2000-12-01", "2000-12-31", freq="D") # 2000-12 (present)
    t_feb = xr.date_range("2001-02-01", "2001-02-28", freq="D") # 2001-01 (missing); DJF incomplete
    t_mam = xr.date_range("2001-03-01", "2001-05-31", freq="D") # 2001-03..2001-05; MAM complete
    time = t_dec.append(t_feb).append(t_mam)

    da = xr.DataArray(np.ones(time.size), coords={"time": time}, dims=["time"])
    mask = check_seasonal_chunk_completeness(da, resample_frequency="QS-DEC", loglevel="DEBUG")

    # Expected seasonal chunk start times under QS-DEC in this range: 2000-12-01 and 2001-03-01
    assert "2000-12-01" in mask.time.dt.strftime("%Y-%m-%d").values
    assert "2001-03-01" in mask.time.dt.strftime("%Y-%m-%d").values

    # DJF (starts 2000-12-01) is incomplete (missing January)
    assert bool(mask.sel(time="2000-12-01").item()) is False
    # MAM (starts 2001-03-01) is complete (Mar, Apr, May present)
    assert bool(mask.sel(time="2001-03-01").item()) is True

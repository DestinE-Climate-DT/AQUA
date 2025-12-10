"""Test for some of the utils"""

import pytest
import xarray as xr
import numpy as np
import pandas as pd
from aqua import Reader
from aqua.core.util import extract_literal_and_numeric, file_is_complete, to_list, convert_data_units
from aqua.core.util import format_realization, extract_attrs, time_to_string
from aqua.core.util.string import strlist_to_phrase, lat_to_phrase
from aqua.core.util.units import multiply_units
from conftest import LOGLEVEL

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

loglevel = LOGLEVEL

@pytest.mark.aqua
def test_convert_data_units():
    """Test the check_data function"""
    data = Reader(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly', loglevel=loglevel).retrieve()
    initial_units = data['tprate'].attrs['units']

    # Dataset test
    data_test = convert_data_units(data=data, var='tprate', units='mm/day', loglevel=loglevel)
    assert data_test['tprate'].attrs['units'] == 'mm/day'

    # DataArray test
    data = data['tprate']
    data_test = convert_data_units(data=data, var='tprate', units='mm/day', loglevel=loglevel)
    # We don't test values since this is done in the test_fixer.py
    assert data_test.attrs['units'] == 'mm/day'
    assert f"Converting units of tprate: from {initial_units} to mm/day" in data_test.attrs['history']

    # Test with no conversion to be done
    data_test = convert_data_units(data=data, var='tprate', units=initial_units, loglevel=loglevel)
    assert data_test.attrs['units'] == initial_units
    assert f"Converting units of tprate: from {initial_units} to mm/day" not in data_test.attrs['history']


# Define a fixture to create a sample netCDF file for testing
@pytest.mark.aqua
class TestFileIsComplete:
    """The File is Complete testing class"""

    @pytest.fixture
    def sample_netcdf(self, tmp_path):
        """Create a sample Dataset and its file"""
        filename = tmp_path / "sample_netcdf.nc"
        data = xr.DataArray(np.random.rand(3, 4, 5), dims=("time", "lat", "lon"))
        time_values = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(3)]
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
        time_values = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i) for i in range(3)]
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
        time_values = [pd.Timestamp("2024-01-01") + pd.Timedelta(days=i * 40) for i in range(3)]
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

@pytest.mark.parametrize("arg, expected", [
    (None, []),                        # Test None
    ([1, 2, 3], [1, 2, 3]),              # Test list (unchanged)
    ((4, 5), [4, 5]),                    # Test tuple
    ({6, 7}, [6, 7]),                    # Test set
    ({"capa": 1, "tonda": 2}, ["capa", "tonda"]),      # Test dictionary (keys)
    ("maccio", ["maccio"]),# Test string (split into chars)
    (8, [8]),                            # Test single integer
    (3.14, [3.14]),                      # Test single float
    (True, [True]),                      # Test single boolean
    ([], []),                            # Test empty list
    ({}, []),                            # Test empty dictionary
    (set(), []),                         # Test empty set
    ((), []),                            # Test empty tuple
])
def test_to_list(arg, expected):
    assert to_list(arg) == expected

@pytest.mark.aqua
def test_format_realization():
    """Test the format_realization function"""
    assert format_realization() == "r1"
    assert format_realization(1) == "r1"
    assert format_realization("2") == "r2"
    assert format_realization("test") == "test"
    assert format_realization("") == "r1"
    assert format_realization(None) == "r1"
    assert format_realization(["1", "2", "test", 3, None]) == ["r1", "r2", "test", "r3", "r1"]

@pytest.mark.aqua
def test_extract_attrs():
    assert extract_attrs(None, "attr") is None # Data is None
    ds_with_attr = xr.Dataset()
    ds_with_attr.attrs = {"attr": "value1"}
    ds_without_attr = xr.Dataset()
    assert extract_attrs(ds_with_attr, "attr") == "value1" # Single dataset with attribute
    assert extract_attrs(ds_without_attr, "attr") is None # Single dataset without attribute
    result = extract_attrs([ds_with_attr, ds_without_attr], "attr") 
    assert result == ["value1", None] # List of datasets

@pytest.mark.aqua
def test_strlist_to_phrase():
    """Test the strlist_to_phrase function"""
    # Test empty list
    assert strlist_to_phrase([]) == ""
    # Test single item
    assert strlist_to_phrase(["A"]) == "A"
    assert strlist_to_phrase(["Hello"]) == "Hello"
    # Test two items
    assert strlist_to_phrase(["A", "B"]) == "A and B"
    assert strlist_to_phrase(["Hello", "World"]) == "Hello and World"
    # Test more than three items
    assert strlist_to_phrase(["A", "B", "C", "D"]) == "A, B, C and D"
    assert strlist_to_phrase(["A", "B", "C", "D"], oxford_comma=True) == "A, B, C, and D"
    # Test with mixed content
    assert strlist_to_phrase(["Mod", "Exp", "Source"]) == "Mod, Exp and Source"
    assert strlist_to_phrase(["Mod", "Exp", "Source"], oxford_comma=True) == "Mod, Exp, and Source"

@pytest.mark.aqua
def test_lat_to_phrase():
    """Test the lat_to_phrase function"""
    # Test northern latitudes
    assert lat_to_phrase(90) == "90°N"
    # Test southern latitudes
    assert lat_to_phrase(-1) == "1°S"
    # Test 0 latitude
    assert lat_to_phrase(0) == "0°N"
    
# Uncomment this test if the flip_time function is uncommented in aqua/util/coord.py
# def test_flip_time():
#     """Test the flip_time function"""
#     time = np.arange(0, 10)
#     data = xr.DataArray(np.random.rand(10, 3, 4), dims=("time", "lat", "lon"))
#     data = data.assign_coords(time=time)
#     flipped_data = flip_time(data)
#     assert flipped_data.time.values[0] == 9
#     assert flipped_data.time.values[-1] == 0

@pytest.mark.aqua
@pytest.mark.parametrize("unit1, unit2, expected", [
    ("m", "m2", "meter ** 3"),                         # Basic area × length = volume
    ("m", "m", "meter ** 2"),                          # Length × length = area
    ("kg", "m/s2", "kilogram * meter / second ** 2"),  # Mass × acceleration = force
    ("m/s", "s", "meter"),                             # Velocity × time = distance
    ("1", "m", "meter"),                               # Dimensionless × length
    ("kg/m3", "m3", "kilogram"),                       # Density × volume = mass
])
def test_multiply_units(unit1, unit2, expected):
    """Test the multiply_units function with various unit combinations"""
    result = multiply_units(unit1, unit2)
    assert result == expected

@pytest.mark.aqua
def test_multiply_units_no_base_conversion():
    """Test multiply_units without converting to base units"""
    result = multiply_units("km", "km", to_base_units=False)
    assert result == "kilometer ** 2"

@pytest.mark.aqua
def test_multiply_units_no_normalization():
    """Test multiply_units without normalization"""
    result = multiply_units("m", "m", normalise_units=False)
    assert result == "meter ** 2"

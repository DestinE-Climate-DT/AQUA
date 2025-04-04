"""Tests for the aqua.datamodel module."""
import xarray as xr
import numpy as np
import pytest
from aqua import Reader
from aqua.datamodel import CoordTransformer

@pytest.mark.aqua
class TestDataModel():

    @pytest.fixture
    def data(self):
        return xr.Dataset(
            {
                "temperature": (["time", "plev", "lat", "lon", "deeepth"], np.random.rand(2, 5, 3, 4, 3)),
            },
            coords={
                "time": ["2023-01-01", "2023-01-02"],
                "level": [1000, 850, 700, 500, 300],  # Pressione in hPa
                "LATITUDE": [10, 20, 30],
                "longi": [100, 110, 120, 130],
                "deeepth": [0, 10, 20],
            },
        )

    def test_basic_transform_vertical(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="FESOM", exp="test-pi", source="original_3d") 
        data = reader.retrieve()
        new = CoordTransformer(data, loglevel='debug').transform_coords()

        assert "depth" in new.coords
        assert "nz1" not in new.coords
        assert "idx_depth" in new.coords

    def test_basic_transform(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="IFS", exp="test-tco79", source="long")
        data = reader.retrieve(var='2t')
        new = CoordTransformer(data, loglevel='debug').transform_coords()

        assert "lon" in new.coords
        assert "X" == new['lon'].attrs["axis"]
        assert "degrees_east" == new['lon'].attrs["units"]

        assert "lat" in new.coords
        assert "Y" == new['lat'].attrs["axis"]
        assert "degrees_north" == new['lat'].attrs["units"]

    def test_bounds(self):
        """Test for bounds fixing and unit conversion."""

        data = xr.open_dataset("AQUA_tests/grids/IFS/tco79_grid.nc")
        new = CoordTransformer(data, loglevel='debug').transform_coords()

        assert "lon_bnds" in new.data_vars
        assert "lat_bnds" in new.data_vars
        assert new["lat"].max().values > 89
        assert new["lat_bnds"].max().values > 89
        assert "degrees_north" == new['lat'].attrs["units"]

    def test_fake_weird_case(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"units": "hPa"}
        data['longi'].attrs = {"units": "degrees_east"}
        data['LATITUDE'].attrs = {"units": "degrees_north"}
        data['deeepth'].attrs = {"standard_name": "depth"}

        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "lat" in new.coords
        assert "lon" in new.coords
        assert "level" not in new.coords
        assert "plev" in new.coords
        assert "Pa" == new["plev"].attrs["units"]
        assert new["plev"].max().values == 100000
        assert "depth" in new.coords

    def test_fake_weird_case_second(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"standard_name": "air_pressure"}
        data['longi'].attrs = {"axis": "X"}
        data['LATITUDE'].attrs = {"axis": "Y"}
        data['deeepth'].attrs = {"long_name": "so much water depth"}

        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "lat" in new.coords
        assert "lon" in new.coords
        assert "plev" in new.coords
        assert "depth" in new.coords


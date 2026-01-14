"""Tests for the aqua.datamodel module."""
import xarray as xr
import numpy as np
import pytest
from aqua import Reader
from aqua.core.data_model import CoordTransformer, CoordIdentifier

@pytest.mark.aqua
class TestDataModel():

    @pytest.fixture
    def data(self):
        return xr.Dataset(
            {
                "temperature": (["level", "lat", "lon", "deeepth", "time"], np.random.rand(5, 3, 4, 3, 2)),
                "wind": (["height", "lat", "lon", "time"], np.random.rand(4, 3, 4, 2)),
            },
            coords={
                "level": [1000, 850, 700, 500, 300], 
                "LATITUDE": [10, 20, 30],
                "longi": [100, 110, 120, 130],
                "deeepth": [0, 10, 20],
                "timing": ["2023-01-01", "2023-01-02"],
                "height": [0, 5, 10, 15],
            },
        )
    
    def test_coords_error(self, data):
        """Error case"""

        with pytest.raises(TypeError, match="coords must be an Xarray Coordinates object."):
            CoordIdentifier(data, loglevel='debug')

        with pytest.raises(TypeError, match="data must be an Xarray Dataset or DataArray object."):
            CoordTransformer(data.coords)

        coord = CoordTransformer(data, loglevel='debug')
        with pytest.raises(TypeError, match="name must be a string."):
            coord.transform_coords(name=123)
        with pytest.raises(FileNotFoundError):
            coord.transform_coords(name="antani")
        
    def test_basic_transform_vertical(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="FESOM", exp="test-pi", source="original_3d", fix=False) 
        data = reader.retrieve()

        # case for multiple vertical coordinates, ignore along the vertical
        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "nz1" in new.coords
        assert "nz" in new.coords

        # case for single vertical coordinate, convert it
        new = CoordTransformer(data['temp'], loglevel='debug').transform_coords()

        assert "depth" in new.coords
        assert "nz1" not in new.coords
        assert "idx_depth" in new.coords

    def test_basic_transform_height(self):
        """Test for height coordinate transformation."""

        reader = Reader(model="ICON", exp="test-r2b0", source="short", loglevel="warning", fix=False)
        data = reader.retrieve()
        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "height" in new.coords

    def test_basic_transform(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="IFS", exp="test-tco79", source="long", fix=False)
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
        data = data.rename({"timing": "time"})

        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "lat" in new.coords
        assert "lon" in new.coords
        assert "level" not in new.coords
        assert "plev" in new.coords
        assert "time" in new.coords
        assert "Pa" == new["plev"].attrs["units"]
        assert new["plev"].max().values == 100000
        assert "depth" in new.coords

    def test_fake_weird_case_second(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"standard_name": "air_pressure"}
        data['timing'].attrs = {"standard_name": "time"}
        data['longi'].attrs = {"axis": "X"}
        data['LATITUDE'].attrs = {"axis": "Y"}
        data['deeepth'].attrs = {"long_name": "so much water depth"}

        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "lat" in new.coords
        assert "lon" in new.coords
        assert "plev" in new.coords
        assert "depth" in new.coords
        assert "time" in new.coords

    def test_fake_weird_case_third(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"units": "patate"}
        data['timing'].attrs = {"axis": "T"}
        data = data.rename({"deeepth": "depth"})

        new = CoordTransformer(data, loglevel='debug').transform_coords()
        assert "time" in new.coords
        assert "depth" in new.coords

    def test_ranking_case(self, data):
        """Test for ranking functionality in CoordIdentifier."""

        data = data.rename({"LATITUDE": "lat"})
        data['longi'].attrs = {"axis": "Y"}
        
        identifier = CoordIdentifier(data.coords, loglevel='debug')
        coord_dict = identifier.identify_coords()

        # Check that only one coordinate is identified for each type
        assert coord_dict['longitude'] is None
        assert coord_dict['latitude']['name'] == 'lat'

    def same_score_ranking_case(self, data):
        """Test for conflict ranking functionality in CoordIdentifier."""

        data = data.rename({"LATITUDE": "lat"})
        data = data.rename({"longi": "latitude"})
        
        identifier = CoordIdentifier(data.coords, loglevel='debug')
        coord_dict = identifier.identify_coords()

        # No coordinate should be identified due to same score
        assert coord_dict['longitude'] is None
        assert coord_dict['latitude'] is None


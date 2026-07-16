"""Tests for the aqua.datamodel module."""

import numpy as np
import pytest
import xarray as xr

from aqua import Reader
from aqua.core.data_model import CoordIdentifier, CoordTransformer


@pytest.mark.aqua
class TestDataModel:
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
            CoordIdentifier(data, loglevel="debug")

        with pytest.raises(TypeError, match="data must be an Xarray Dataset or DataArray object."):
            CoordTransformer(data.coords)

        coord = CoordTransformer(data, loglevel="debug")
        with pytest.raises(TypeError, match="name must be a string."):
            coord.transform_coords(name=123)
        with pytest.raises(FileNotFoundError):
            coord.transform_coords(name="antani")

    def test_basic_transform_vertical(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="FESOM", exp="test-pi", source="original_3d", fix=False)
        data = reader.retrieve()

        # case for multiple vertical coordinates, ignore along the vertical
        new = CoordTransformer(data, loglevel="debug").transform_coords()
        assert "nz1" in new.coords
        assert "nz" in new.coords

        # case for single vertical coordinate, convert it
        new = CoordTransformer(data["temp"], loglevel="debug").transform_coords()

        assert "depth" in new.coords
        assert "nz1" not in new.coords

    def test_basic_transform_height(self):
        """Test for height coordinate transformation."""

        reader = Reader(model="ICON", exp="test-r2b0", source="short", loglevel="warning", fix=False)
        data = reader.retrieve()
        new = CoordTransformer(data, loglevel="debug").transform_coords()
        assert "height" in new.coords

    def test_basic_transform(self):
        """Basic test for the CoordTransformer class."""

        reader = Reader(model="IFS", exp="test-tco79", source="long", fix=False)
        data = reader.retrieve(var="2t")

        new = CoordTransformer(data, loglevel="debug").transform_coords()

        assert "lon" in new.coords
        assert "X" == new["lon"].attrs["axis"]
        assert "degrees_east" == new["lon"].attrs["units"]

        assert "lat" in new.coords
        assert "Y" == new["lat"].attrs["axis"]
        assert "degrees_north" == new["lat"].attrs["units"]

    def test_bounds(self):
        """Test for bounds fixing and unit conversion."""

        data = xr.open_dataset("AQUA_tests/grids/IFS/tco79_grid.nc")
        new = CoordTransformer(data, loglevel="debug").transform_coords()

        assert "lon_bnds" in new.data_vars
        assert "lat_bnds" in new.data_vars
        assert new["lat"].max().values > 89
        assert new["lat_bnds"].max().values > 89
        assert "degrees_north" == new["lat"].attrs["units"]

    def test_fake_weird_case(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"units": "hPa"}
        data["longi"].attrs = {"units": "degrees_east"}
        data["LATITUDE"].attrs = {"units": "degrees_north"}
        data["deeepth"].attrs = {"standard_name": "depth"}
        data = data.rename({"timing": "time"})

        new = CoordTransformer(data, loglevel="debug").transform_coords()
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
        data["timing"].attrs = {"standard_name": "time"}
        data["longi"].attrs = {"axis": "X"}
        data["LATITUDE"].attrs = {"axis": "Y"}
        data["deeepth"].attrs = {"long_name": "so much water depth"}

        new = CoordTransformer(data, loglevel="debug").transform_coords()
        assert "lat" in new.coords
        assert "lon" in new.coords
        assert "plev" in new.coords
        assert "depth" in new.coords
        assert "time" in new.coords

    def test_fake_weird_case_third(self, data):
        """Test for more complex cases."""

        data["level"].attrs = {"units": "patate"}
        data["timing"].attrs = {"axis": "T"}
        data = data.rename({"deeepth": "depth"})

        new = CoordTransformer(data, loglevel="debug").transform_coords()
        assert "time" in new.coords
        assert "depth" in new.coords

    def test_ranking_case(self, data):
        """Test for ranking functionality in CoordIdentifier."""

        data = data.rename({"LATITUDE": "lat"})
        data["longi"].attrs = {"axis": "Y"}

        identifier = CoordIdentifier(data.coords, loglevel="debug")
        coord_dict = identifier.identify_coords()

        # Check that only one coordinate is identified for each type
        assert coord_dict["longitude"] is None
        assert coord_dict["latitude"]["name"] == "lat"

    def same_score_ranking_case(self, data):
        """Test for conflict ranking functionality in CoordIdentifier."""

        data = data.rename({"LATITUDE": "lat"})
        data = data.rename({"longi": "latitude"})

        identifier = CoordIdentifier(data.coords, loglevel="debug")
        coord_dict = identifier.identify_coords()

        # No coordinate should be identified due to same score
        assert coord_dict["longitude"] is None
        assert coord_dict["latitude"] is None


@pytest.mark.aqua
class TestLongitudeRangeNormalization:
    """
    Tests for CoordTransformer.normalize_longitude_range().

    Covers the fix for the bug where longitude convention (0-360 vs -180/180)
    was never enforced by the data model: area files generated by CDO stayed
    in -180/180 while retrieved data kept the source's native 0-360, causing
    a false 'Mismatch in values for coordinate lon' error in FldStat even
    though both datasets described the same physical grid.

    The "aqua" data model declares range: [0, 360] for longitude, so these
    tests assume that convention unless otherwise noted.
    """

    @staticmethod
    def _lon_dataset(lon_values):
        """Minimal dataset with a plain-named longitude/latitude, CF-style units."""
        ds = xr.Dataset(
            {"temperature": (["latitude", "longitude"], np.random.rand(2, len(lon_values)))},
            coords={"latitude": [10.0, 20.0], "longitude": lon_values},
        )
        ds["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
        ds["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
        return ds

    def test_wraps_negative_lon_into_0_360(self):
        """
        End-to-end via transform_coords(): a dataset with lon in [-180, 180]
        must come out wrapped into [0, 360) since aqua.yaml declares that range.
        """
        lon = np.array([-180.0, -90.0, 0.0, 90.0, 179.0])
        ds = self._lon_dataset(lon)

        new = CoordTransformer(ds, loglevel="debug").transform_coords()

        assert new.lon.values.min() >= 0
        assert new.lon.values.max() < 360
        # -180 and 180 are the same point; -90 -> 270, 0 -> 0, 90 -> 90, 179 -> 179
        np.testing.assert_allclose(new.lon.values, [180.0, 270.0, 0.0, 90.0, 179.0])
        # 'range' is a wrap-convention control key, must not leak into file attrs
        assert "range" not in new.lon.attrs

    def test_already_in_range_is_noop(self):
        """Longitude already within the declared range should be left untouched."""
        lon = np.array([0.0, 90.0, 180.0, 270.0, 359.0])
        ds = self._lon_dataset(lon)

        new = CoordTransformer(ds, loglevel="debug").transform_coords()

        np.testing.assert_array_equal(new.lon.values, lon)

    def test_direct_call_wraps_lon(self):
        """Unit test calling normalize_longitude_range directly (bypasses rename/identify)."""
        ds = xr.Dataset(coords={"lon": [-170.0, -10.0, 10.0, 170.0]})
        transformer = CoordTransformer(self._lon_dataset(np.array([0.0])), loglevel="debug")

        tgt_coord = {"name": "lon", "range": "0_360"}
        result = transformer.normalize_longitude_range(ds, tgt_coord)

        np.testing.assert_allclose(result.lon.values, [190.0, 350.0, 10.0, 170.0])

    def test_bounds_wrapped_alongside_coordinate(self):
        """lon_bnds must be wrapped using the same convention as lon itself."""
        lon = np.array([-170.0, -10.0])
        bnds = np.array([[-180.0, -160.0], [-20.0, 0.0]])
        ds = xr.Dataset(
            {"lon_bnds": (["lon", "nv"], bnds)},
            coords={"lat": [10.0, 20.0], "lon": lon},
        )
        transformer = CoordTransformer(self._lon_dataset(np.array([0.0])), loglevel="debug")

        tgt_coord = {"name": "lon", "range": "0_360", "bounds": "lon_bnds"}
        result = transformer.normalize_longitude_range(ds, tgt_coord)

        assert result.lon_bnds.values.min() >= 0
        assert result.lon_bnds.values.max() < 360
        np.testing.assert_allclose(result.lon_bnds.values, [[180.0, 200.0], [340.0, 0.0]])

    def test_no_range_declared_is_noop(self):
        """
        Without a 'range' key in the target coordinate dict, normalize_longitude_range
        must be a pure no-op. This is required for backward compatibility with any
        data model that doesn't declare a longitude convention.
        """
        ds = xr.Dataset(coords={"lon": [-180.0, -90.0, 0.0]})
        transformer = CoordTransformer(self._lon_dataset(np.array([0.0])), loglevel="debug")

        tgt_coord = {"name": "lon"}  # no "range" key
        result = transformer.normalize_longitude_range(ds, tgt_coord)

        np.testing.assert_array_equal(result.lon.values, [-180.0, -90.0, 0.0])

    def test_non_longitude_coordinate_untouched(self):
        """normalize_longitude_range should only ever act on lon/longitude."""
        ds = xr.Dataset(coords={"latitude": [-180.0, 0.0, 90.0]})
        transformer = CoordTransformer(self._lon_dataset(np.array([0.0])), loglevel="debug")

        # even with a 'range' declared, a non-longitude coord must be left alone
        tgt_coord = {"name": "latitude", "range": "0_360"}
        result = transformer.normalize_longitude_range(ds, tgt_coord)

        np.testing.assert_array_equal(result.latitude.values, [-180.0, 0.0, 90.0])

    def test_missing_coordinate_is_noop(self):
        """If the target coordinate isn't present in data, nothing should happen or raise."""
        ds = xr.Dataset(coords={"lat": [0.0, 10.0]})
        transformer = CoordTransformer(self._lon_dataset(np.array([0.0])), loglevel="debug")

        tgt_coord = {"name": "lon", "range": "0_360"}
        result = transformer.normalize_longitude_range(ds, tgt_coord)

        assert "lon" not in result.coords

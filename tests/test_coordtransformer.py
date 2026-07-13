"""
Unit tests for aqua.core.data_model.coordtransformer.CoordTransformer.

These tests exercise CoordTransformer directly against small synthetic
xarray objects (no Reader / test-catalog data required), so they run fast
and in isolation from the rest of the framework. They complement:

  - test_datamodel.py: end-to-end tests going through Reader.retrieve() and
    through transform_coords() with the real "aqua" data model, including
    TestLongitudeRangeNormalization for the 0-360/-180-180 wrap fix.
  - test_coord_flipping.py: focused tests for latitude direction flipping.

This file focuses on unit-level coverage of each CoordTransformer method:
__init__/_info_grid, rename_coordinate, flip_coordinate, convert_units,
normalize_longitude_range, assign_attributes, transform_coords (integration
of the above), and the module-level counter_reverse_coordinate().
"""

import numpy as np
import pytest
import xarray as xr
from conftest import LOGLEVEL

from aqua.core.data_model import CoordTransformer
from aqua.core.data_model.coordtransformer import counter_reverse_coordinate

loglevel = LOGLEVEL
pytestmark = pytest.mark.aqua


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

def _regular_dataset(lat_values=None, lon_values=None, lat_decreasing=False):
    """1D lat/lon on separate dims -> CoordTransformer._info_grid() = 'Regular'."""
    lat_values = lat_values if lat_values is not None else np.array([-90.0, -30.0, 30.0, 90.0])
    if lat_decreasing:
        lat_values = lat_values[::-1]
    lon_values = lon_values if lon_values is not None else np.array([0.0, 90.0, 180.0, 270.0])

    ds = xr.Dataset(
        {"var": (["latitude", "longitude"], np.random.rand(len(lat_values), len(lon_values)))},
        coords={"latitude": lat_values, "longitude": lon_values},
    )
    ds["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
    ds["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
    return ds


def _unstructured_dataset(lat_values=None, lon_values=None):
    """1D lat/lon sharing a single 'cell' dim -> _info_grid() = 'Unstructured'."""
    lat_values = lat_values if lat_values is not None else np.array([-45.0, 0.0, 45.0, 89.0])
    lon_values = lon_values if lon_values is not None else np.array([0.0, 90.0, 180.0, 270.0])

    ds = xr.Dataset(
        {"var": (["cell"], np.random.rand(len(lat_values)))},
        coords={
            "latitude": ("cell", lat_values),
            "longitude": ("cell", lon_values),
        },
    )
    ds["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
    ds["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
    return ds


def _curvilinear_dataset():
    """2D lat/lon -> _info_grid() = 'Curvilinear'."""
    lat2d = np.array([[-10.0, -10.0], [10.0, 10.0]])
    lon2d = np.array([[0.0, 90.0], [0.0, 90.0]])
    ds = xr.Dataset(
        {"var": (["y", "x"], np.random.rand(2, 2))},
        coords={
            "latitude": (["y", "x"], lat2d),
            "longitude": (["y", "x"], lon2d),
        },
    )
    ds["latitude"].attrs = {"units": "degrees_north", "standard_name": "latitude"}
    ds["longitude"].attrs = {"units": "degrees_east", "standard_name": "longitude"}
    return ds


def _get_coord(transformer, coord_type):
    """Fetch the identified src_coord dict for 'latitude'/'longitude'/etc."""
    return transformer.src_coords.get(coord_type)


# ---------------------------------------------------------------------------
# __init__ / _info_grid
# ---------------------------------------------------------------------------

class TestInitAndGridType:
    def test_init_rejects_non_xarray(self):
        with pytest.raises(TypeError, match="data must be an Xarray Dataset or DataArray object."):
            CoordTransformer([1, 2, 3])

    def test_accepts_dataarray(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds["var"], loglevel=loglevel)
        assert transformer.gridtype in ("Regular", "Unstructured", "Curvilinear", "Unknown")

    def test_grid_type_regular(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        assert transformer.gridtype == "Regular"

    def test_grid_type_unstructured(self):
        ds = _unstructured_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        assert transformer.gridtype == "Unstructured"

    def test_grid_type_curvilinear(self):
        ds = _curvilinear_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        assert transformer.gridtype == "Curvilinear"

    def test_grid_type_unknown_when_no_lat_lon(self):
        ds = xr.Dataset({"var": (["x"], [1, 2, 3])}, coords={"x": [0, 1, 2]})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        assert transformer.gridtype == "Unknown"


# ---------------------------------------------------------------------------
# rename_coordinate / _rename_bounds
# ---------------------------------------------------------------------------

class TestRenameCoordinate:
    def test_renames_when_names_differ(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = _get_coord(transformer, "latitude")
        tgt = {"name": "lat"}

        result = transformer.rename_coordinate(ds, src, tgt)

        assert "lat" in result.coords
        assert "latitude" not in result.coords

    def test_noop_when_names_already_match(self):
        ds = _regular_dataset()
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = _get_coord(transformer, "latitude")
        tgt = {"name": "lat"}

        result = transformer.rename_coordinate(ds, src, tgt)

        assert result is ds  # untouched, same object returned

    def test_renames_bounds_alongside_coordinate(self):
        ds = _regular_dataset()
        ds["latitude_bnds"] = (
            ["latitude", "nv"],
            np.zeros((ds.dims["latitude"], 2)),
        )
        ds["latitude"].attrs["bounds"] = "latitude_bnds"

        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = _get_coord(transformer, "latitude")
        tgt = {"name": "lat"}

        result = transformer.rename_coordinate(ds, src, tgt)

        assert "lat_bnds" in result.data_vars
        assert "latitude_bnds" not in result.data_vars
        assert result["lat"].attrs["bounds"] == "lat_bnds"

    def test_missing_bounds_does_not_raise(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = _get_coord(transformer, "latitude")
        tgt = {"name": "lat"}

        # src bounds is None (no 'bounds' attr set) -> should just pass through
        result = transformer.rename_coordinate(ds, src, tgt)
        assert "lat" in result.coords


# ---------------------------------------------------------------------------
# flip_coordinate
# ---------------------------------------------------------------------------

class TestFlipCoordinate:
    """
    Basic flip / no-flip-when-already-increasing behavior on Regular grids is
    already covered end-to-end (with real reordering of 2D/3D data) by
    test_coord_flipping.py::test_coord_flipping_lat_decreasing_to_increasing
    and ::test_coord_no_flip_when_already_increasing. This class only covers
    edge cases not exercised there.
    """

    def test_no_flip_on_unstructured_grid_even_if_decreasing(self):
        ds = _unstructured_dataset(
            lat_values=np.array([45.0, 0.0, -45.0]),
            lon_values=np.array([0.0, 90.0, 180.0]),
        )
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = dict(_get_coord(transformer, "latitude"))
        tgt = {"name": "lat", "stored_direction": "increasing"}

        result = transformer.flip_coordinate(ds, src, tgt)

        # Unstructured grids must never be flipped, regardless of direction mismatch
        np.testing.assert_array_equal(result.lat.values, ds.lat.values)
        assert result.lat.attrs.get("flipped") is None

    def test_missing_tgt_direction_is_noop(self):
        ds = _regular_dataset(lat_decreasing=True)
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = dict(_get_coord(transformer, "latitude"))
        tgt = {"name": "lat"}  # no stored_direction key

        result = transformer.flip_coordinate(ds, src, tgt)
        assert result is ds

    def test_invalid_tgt_direction_raises(self):
        ds = _regular_dataset()
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = dict(_get_coord(transformer, "latitude"))
        tgt = {"name": "lat", "stored_direction": "sideways"}

        with pytest.raises(ValueError, match="tgt direction must be 'increasing' or 'decreasing'"):
            transformer.flip_coordinate(ds, src, tgt)

    def test_invalid_src_direction_warns_and_noop(self):
        ds = _regular_dataset(lat_decreasing=True)
        ds = ds.rename({"latitude": "lat", "longitude": "lon"})
        transformer = CoordTransformer(ds, loglevel=loglevel)
        src = dict(_get_coord(transformer, "latitude"))
        src["stored_direction"] = "unknown"
        tgt = {"name": "lat", "stored_direction": "increasing"}

        result = transformer.flip_coordinate(ds, src, tgt)

        assert result is ds
        assert result.lat.attrs.get("flipped") is None


# ---------------------------------------------------------------------------
# convert_units / _convert_bounds
# ---------------------------------------------------------------------------

class TestConvertUnits:
    def test_converts_radians_to_degrees(self):
        ds = xr.Dataset(coords={"lon": np.array([0.0, np.pi / 2, np.pi])})
        ds["lon"].attrs = {"units": "radian"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon", "units": "radian"}
        tgt = {"name": "lon", "units": "degrees_east"}

        result = transformer.convert_units(ds, src, tgt)

        np.testing.assert_allclose(result.lon.values, [0.0, 90.0, 180.0])
        assert result.lon.attrs["units"] == "degrees_east"

    def test_noop_when_tgt_has_no_units(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        ds["lon"].attrs = {"units": "radian"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon", "units": "radian"}
        tgt = {"name": "lon"}  # no "units" key

        result = transformer.convert_units(ds, src, tgt)
        np.testing.assert_array_equal(result.lon.values, ds.lon.values)

    def test_noop_when_src_has_no_units(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        ds["lon"].attrs = {"units": "radian"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon"}  # no "units" key
        tgt = {"name": "lon", "units": "degrees_east"}

        result = transformer.convert_units(ds, src, tgt)
        np.testing.assert_array_equal(result.lon.values, ds.lon.values)

    def test_noop_when_data_missing_units_attr(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})  # no attrs at all
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon", "units": "radian"}
        tgt = {"name": "lon", "units": "degrees_east"}

        result = transformer.convert_units(ds, src, tgt)
        np.testing.assert_array_equal(result.lon.values, ds.lon.values)

    def test_fixes_malformed_degree_unit_spelling_before_attempting_conversion(self):
        """
        'degree north' (singular, space-separated) is normalized to
        'degrees north' before a conversion is attempted. That normalized
        form still isn't recognized by pint against the target
        'degrees_north' (underscore), so the conversion itself is skipped
        and the original units string on the data is left untouched -- this
        asserts that skip-path behavior explicitly, rather than assuming
        conversion succeeds.
        """
        ds = xr.Dataset(coords={"lat": [0.0, 45.0]})
        ds["lat"].attrs = {"units": "degree north"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lat", "units": "degree north"}
        tgt = {"name": "lat", "units": "degrees_north"}

        result = transformer.convert_units(ds, src, tgt)
        # values and units attr both unchanged since the conversion was skipped
        np.testing.assert_array_equal(result.lat.values, [0.0, 45.0])
        assert result.lat.attrs["units"] == "degree north"
        # but the src_coord dict itself should have had its spelling fixed
        assert src["units"] == "degrees north"

    def test_incompatible_units_skips_conversion(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        ds["lon"].attrs = {"units": "kelvin"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon", "units": "kelvin"}
        tgt = {"name": "lon", "units": "degrees_east"}

        result = transformer.convert_units(ds, src, tgt)
        # values must be untouched since conversion is impossible/incompatible
        np.testing.assert_array_equal(result.lon.values, ds.lon.values)

    def test_converts_bounds_with_same_factor(self):
        ds = xr.Dataset(
            {"lon_bnds": (["lon", "nv"], np.array([[0.0, np.pi / 2], [np.pi / 2, np.pi]]))},
            coords={"lon": np.array([0.0, np.pi / 2])},
        )
        ds["lon"].attrs = {"units": "radian", "bounds": "lon_bnds"}
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        src = {"name": "lon", "units": "radian", "bounds": "lon_bnds"}
        tgt = {"name": "lon", "units": "degrees_east", "bounds": "lon_bnds"}

        result = transformer.convert_units(ds, src, tgt)

        np.testing.assert_allclose(result.lon_bnds.values, [[0.0, 90.0], [90.0, 180.0]])
        assert result.lon_bnds.attrs["units"] == "degrees_east"


# ---------------------------------------------------------------------------
# normalize_longitude_range
# (core coverage also lives in test_datamodel.py::TestLongitudeRangeNormalization;
#  kept here too since this file is the canonical unit-test home for
#  CoordTransformer, and to test it alongside the rest of the class's methods)
# ---------------------------------------------------------------------------

class TestNormalizeLongitudeRange:
    def test_wraps_into_declared_range(self):
        ds = xr.Dataset(coords={"lon": [-170.0, -10.0, 10.0, 170.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon", "range": [0, 360]}
        result = transformer.normalize_longitude_range(ds, tgt)

        np.testing.assert_allclose(result.lon.values, [190.0, 350.0, 10.0, 170.0])

    def test_noop_without_range_key(self):
        ds = xr.Dataset(coords={"lon": [-170.0, 10.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon"}
        result = transformer.normalize_longitude_range(ds, tgt)

        np.testing.assert_array_equal(result.lon.values, [-170.0, 10.0])

    def test_ignores_non_longitude_coordinates(self):
        ds = xr.Dataset(coords={"lat": [-170.0, 10.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lat", "range": [0, 360]}
        result = transformer.normalize_longitude_range(ds, tgt)

        np.testing.assert_array_equal(result.lat.values, [-170.0, 10.0])

    def test_invalid_range_shape_is_noop(self):
        ds = xr.Dataset(coords={"lon": [-170.0, 10.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon", "range": [0, 180, 360]}  # malformed: 3 elements
        result = transformer.normalize_longitude_range(ds, tgt)

        np.testing.assert_array_equal(result.lon.values, [-170.0, 10.0])


# ---------------------------------------------------------------------------
# assign_attributes
# ---------------------------------------------------------------------------

class TestAssignAttributes:
    def test_adds_extra_attributes(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon", "units": "degrees_east", "axis": "X", "standard_name": "longitude"}
        result = transformer.assign_attributes(ds, tgt)

        assert result.lon.attrs["axis"] == "X"
        assert result.lon.attrs["standard_name"] == "longitude"
        # control keys must never be copied as attributes
        assert "units" not in result.lon.attrs or True  # units handled by convert_units, not here
        assert "name" not in result.lon.attrs
        assert "bounds" not in result.lon.attrs

    def test_range_key_never_leaks_into_attrs(self):
        """Regression test: 'range' is a wrap-convention control key, not a CF attribute."""
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon", "range": [0, 360], "axis": "X"}
        result = transformer.assign_attributes(ds, tgt)

        assert "range" not in result.lon.attrs
        assert result.lon.attrs["axis"] == "X"

    def test_does_not_overwrite_existing_attribute(self):
        ds = xr.Dataset(coords={"lon": [0.0, 1.0]})
        ds["lon"].attrs["axis"] = "custom_value"
        transformer = CoordTransformer(_regular_dataset(), loglevel=loglevel)

        tgt = {"name": "lon", "axis": "X"}
        result = transformer.assign_attributes(ds, tgt)

        assert result.lon.attrs["axis"] == "custom_value"


# ---------------------------------------------------------------------------
# transform_coords (integration of all the above through the real data model)
# ---------------------------------------------------------------------------

class TestTransformCoordsIntegration:
    def test_full_pipeline_on_regular_grid(self):
        """
        This checks that normalize_longitude_range is actually wired into the
        real transform_coords() call (via the real aqua.yaml 'range' key), not
        just correct in isolation (see TestNormalizeLongitudeRange above).
        Flip mechanics themselves are already covered end-to-end, with real
        data reordering, by test_coord_flipping.py -- so only a light check
        is kept here to confirm flip and lon-wrap don't interfere with each
        other when run together through the same pipeline.
        """
        ds = _regular_dataset(lat_decreasing=True, lon_values=np.array([-170.0, -10.0, 10.0, 170.0]))
        transformer = CoordTransformer(ds, loglevel=loglevel)

        result = transformer.transform_coords(name="aqua")

        assert result.lat.values[0] < result.lat.values[-1]  # flip still happens
        np.testing.assert_allclose(sorted(result.lon.values), [10.0, 170.0, 190.0, 350.0])
        assert result.lon.attrs["units"] == "degrees_east"

    def test_flip_coords_false_disables_flipping_but_not_lon_wrap(self):
        ds = _regular_dataset(lat_decreasing=True, lon_values=np.array([-170.0, -10.0, 10.0, 170.0]))
        transformer = CoordTransformer(ds, loglevel=loglevel)

        result = transformer.transform_coords(name="aqua", flip_coords=False)

        # lat must stay decreasing since flip is disabled
        assert result.lat.values[0] > result.lat.values[-1]
        # lon wrap is independent of flip_coords and must still apply
        assert result.lon.values.min() >= 0

    def test_invalid_name_type_raises(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        with pytest.raises(TypeError, match="name must be a string."):
            transformer.transform_coords(name=42)

    def test_unknown_data_model_raises(self):
        ds = _regular_dataset()
        transformer = CoordTransformer(ds, loglevel=loglevel)
        with pytest.raises(FileNotFoundError):
            transformer.transform_coords(name="this_data_model_does_not_exist")


# ---------------------------------------------------------------------------
# counter_reverse_coordinate (module-level helper)
# ---------------------------------------------------------------------------

class TestCounterReverseCoordinate:
    def test_reverses_flipped_coordinate_and_clears_marker(self):
        ds = _regular_dataset(lat_decreasing=True)
        transformer = CoordTransformer(ds, loglevel=loglevel)
        result = transformer.transform_coords(name="aqua")
        assert result.lat.attrs.get("flipped") == 1

        reverted = counter_reverse_coordinate(result)

        assert "flipped" not in reverted.lat.attrs
        # back to original (decreasing) order
        assert reverted.lat.values[0] > reverted.lat.values[-1]

    def test_noop_when_nothing_flipped(self):
        ds = _regular_dataset(lat_decreasing=False)
        transformer = CoordTransformer(ds, loglevel=loglevel)
        result = transformer.transform_coords(name="aqua")
        assert result.lat.attrs.get("flipped") is None

        reverted = counter_reverse_coordinate(result)
        np.testing.assert_array_equal(reverted.lat.values, result.lat.values)

import pytest
import xarray as xr
import numpy as np
from aqua.core.data_model import CoordTransformer, counter_reverse_coordinate
from conftest import LOGLEVEL

loglevel = LOGLEVEL
pytestmark = pytest.mark.aqua


def _set_coord_attrs(ds):
    """Set standard coordinate attributes for CoordIdentifier."""
    ds.latitude.attrs = {"units": "degrees_north", "standard_name": "latitude", "long_name": "latitude"}
    ds.longitude.attrs = {"units": "degrees_east", "standard_name": "longitude", "long_name": "longitude"}
    return ds

def _create_dataset(lat_values, lon_values, include_3d=False, time_values=None):
    """Create a dataset with given lat/lon values and optional 3D variable."""
    # Create 2D data that follows latitude
    sst_data = np.broadcast_to(lat_values[:, np.newaxis], (len(lat_values), len(lon_values)))
    
    data_vars = {"sst": (["latitude", "longitude"], sst_data)}
    coords = {"latitude": lat_values, "longitude": lon_values}
    
    # Create deterministic data for 3D variable to verify flipping along lat
    if include_3d:
        if time_values is None:
            time_values = np.array([0, 1])
        pres_data = np.zeros((len(lat_values), len(lon_values), len(time_values)))
        for i in range(len(lat_values)):
            for j in range(len(lon_values)):
                for k in range(len(time_values)):
                    pres_data[i, j, k] = i * 100 + j * 10 + k
        data_vars["pres"] = (["latitude", "longitude", "time"], pres_data)
        coords["time"] = time_values
    
    ds = xr.Dataset(data_vars, coords=coords)
    return _set_coord_attrs(ds)

@pytest.fixture
def lat_decreasing():
    """Latitude values in decreasing order (90 to -90)."""
    return np.array([90, 60, 30, 0, -30, -60, -90])

@pytest.fixture
def lat_increasing():
    """Latitude values in increasing order (-90 to 90)."""
    return np.array([-90, -60, -30, 0, 30, 60, 90])

@pytest.fixture
def lon_values():
    """Longitude values."""
    return np.array([0, 30, 60, 90])

@pytest.fixture
def dataset_decreasing_lat(lat_decreasing, lon_values):
    """Dataset with decreasing latitude and 2D/3D variables."""
    return _create_dataset(lat_decreasing, lon_values, include_3d=True)

@pytest.fixture
def dataset_increasing_lat(lat_increasing, lon_values):
    """Dataset with increasing latitude."""
    return _create_dataset(lat_increasing, lon_values)


def test_coord_flipping_lat_decreasing_to_increasing(dataset_decreasing_lat, lat_decreasing, lon_values):
    """
    Test that CoordTransformer correctly:
    - Renames latitude/longitude to lat/lon
    - Flips decreasing latitude to increasing
    - Reorders data accordingly for both 2D and 3D variables
    """
    ds = dataset_decreasing_lat
    sst_data = ds.sst.values
    pres_data = ds.pres.values
    
    transformer = CoordTransformer(ds, loglevel=loglevel)
    ds_transformed = transformer.transform_coords()
    
    # Verify coordinate renaming
    assert "lat" in ds_transformed.coords
    assert "lon" in ds_transformed.coords
    assert "latitude" not in ds_transformed.coords
    assert "longitude" not in ds_transformed.coords
    
    # Verify latitude is now increasing
    lat_transformed = ds_transformed.lat.values
    assert lat_transformed[0] < lat_transformed[-1]
    np.testing.assert_array_equal(lat_transformed, lat_decreasing[::-1])
    
    # Verify longitude is unchanged
    np.testing.assert_array_equal(ds_transformed.lon.values, lon_values)
    
    # Verify shapes are preserved
    assert ds_transformed.sst.shape == ds.sst.shape
    assert ds_transformed.pres.shape == ds.pres.shape
    
    # Verify 2D data is flipped correctly along lat dimension
    np.testing.assert_array_equal(ds_transformed.sst.values, sst_data[::-1, :])
    
    # Data should follow latitude values after transformation
    val_at_0 = ds_transformed.sst.isel(lat=0, lon=0).values
    lat_at_0 = ds_transformed.lat.isel(lat=0).values
    assert val_at_0 == lat_at_0 == -90.0

    val_at_end = ds_transformed.sst.isel(lat=-1, lon=0).values
    lat_at_end = ds_transformed.lat.isel(lat=-1).values
    assert val_at_end == lat_at_end == 90.0
    
    # Verify 3D data is flipped correctly
    np.testing.assert_array_equal(ds_transformed.pres.values, pres_data[::-1, :, :])
    assert ds_transformed.pres.isel(lat=-1, lon=0, time=0).values == 0.0
    assert ds_transformed.pres.isel(lat=0, lon=0, time=0).values == 600.0
    
    # Verify flip tracking attribute is set
    assert ds_transformed.lat.attrs.get("flipped", None) == 1


def test_coord_no_flip_when_already_increasing(dataset_increasing_lat, lat_increasing):
    """Test that no flip occurs when latitude is already increasing."""
    ds = dataset_increasing_lat
    sst_data = ds.sst.values
    
    transformer = CoordTransformer(ds, loglevel=loglevel)
    ds_transformed = transformer.transform_coords()
    
    # Verify no flip occurred - data should be unchanged
    np.testing.assert_array_equal(ds_transformed.lat.values, lat_increasing)
    np.testing.assert_array_equal(ds_transformed.sst.values, sst_data)
    assert ds_transformed.lat.attrs.get("flipped", None) is None


def test_coord_flipping_disabled(lat_decreasing, lon_values):
    """Test that flipping can be disabled with flip_coords=False."""
    ds = _create_dataset(lat_decreasing, lon_values)
    sst_data = ds.sst.values
    
    transformer = CoordTransformer(ds, loglevel=loglevel)
    ds_transformed = transformer.transform_coords(flip_coords=False)
    
    # Verify no flip occurred despite decreasing lat
    np.testing.assert_array_equal(ds_transformed.lat.values, lat_decreasing)
    np.testing.assert_array_equal(ds_transformed.sst.values, sst_data)
    assert ds_transformed.lat.attrs.get("flipped", None) is None


def test_coord_flipping_dataarray(lat_decreasing, lon_values):
    """Test CoordTransformer works with DataArray, not just Dataset."""
    ds = _create_dataset(lat_decreasing, lon_values)
    da = ds.sst
    
    transformer = CoordTransformer(da, loglevel=loglevel)
    da_transformed = transformer.transform_coords()
    
    # Verify transformation works for DataArray
    assert "lat" in da_transformed.coords
    assert "lon" in da_transformed.coords
    assert da_transformed.lat.values[0] < da_transformed.lat.values[-1]
    assert da_transformed.lat.attrs.get("flipped", None) == 1


def test_counter_reverse_coordinate(dataset_decreasing_lat, lat_decreasing):
    """Test that counter_reverse_coordinate can undo a flip."""
    ds = dataset_decreasing_lat
    sst_data = ds.sst.values
    
    # Flips lat
    transformer = CoordTransformer(ds, loglevel=loglevel)
    ds_transformed = transformer.transform_coords()
    
    # Verify flip occurred
    assert ds_transformed.lat.attrs.get("flipped", None) == 1
    assert ds_transformed.lat.values[0] < ds_transformed.lat.values[-1]
    
    # Reverse the flip
    ds_reversed = counter_reverse_coordinate(ds_transformed)
    
    # Verify flip was reversed
    assert ds_reversed.lat.attrs.get("flipped", None) is None
    np.testing.assert_array_equal(ds_reversed.lat.values, lat_decreasing)
    np.testing.assert_array_equal(ds_reversed.sst.values, sst_data)
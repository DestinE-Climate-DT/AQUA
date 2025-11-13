import numpy as np
import xarray as xr
import pytest
import dask.array as da
from aqua import histogram

@pytest.fixture
def sample_data():
    """
    Fixture to create a sample xarray.DataArray for testing.
    """
    lats = np.arange(0, 10, 1)
    lons = np.arange(0, 10, 1)
    data = xr.DataArray(np.random.rand(10, 10), coords=[('lat', lats), ('lon', lons)], name='test_data')
    data.attrs['units'] = 'm'
    data.attrs['long_name'] = 'Test Data'
    return data

@pytest.fixture
def sample_dask_data(sample_data):
    """
    Fixture to create a sample xarray.DataArray with a dask array for testing.
    """
    data = sample_data.copy()
    data.data = da.from_array(data.data, chunks=(5, 5))
    return data

@pytest.fixture
def sample_dataset(sample_data):
    """
    Fixture to create a sample xarray.Dataset for testing.
    """
    ds = sample_data.to_dataset()
    ds['second_var'] = xr.DataArray(np.random.rand(10, 10), coords=sample_data.coords)
    return ds

@pytest.mark.aqua
def test_histogram_basic(sample_data):
    """
    Test the basic functionality of the histogram function.
    """
    hist = histogram(sample_data, bins=5, check=True, range=(0, 1), weighted=False)
    assert isinstance(hist, xr.DataArray)
    assert hist.name == 'histogram'
    assert 'center_of_bin' in hist.coords
    assert int(hist.sum()) == sample_data.size

@pytest.mark.aqua
def test_histogram_density(sample_data):
    """
    Test the histogram function with density=True.
    """
    hist = histogram(sample_data, bins=5, density=True, range=(0, 1), weighted=False)
    assert isinstance(hist, xr.DataArray)
    assert hist.name == 'pdf'
    # The integral of the PDF should be close to 1
    assert np.isclose(hist.sum() * (hist.center_of_bin[1] - hist.center_of_bin[0]), 1, atol=1e-5)

@pytest.mark.aqua
def test_histogram_weighted(sample_data):
    """
    Test the histogram function with weighted=True.
    """
    hist_unweighted = histogram(sample_data, bins=5, weighted=False, check=True, range=(0, 1))
    hist_weighted = histogram(sample_data, bins=5, weighted=True, check=True, range=(0, 1))
    assert not np.allclose(hist_unweighted.values, hist_weighted.values)

@pytest.mark.aqua
def test_histogram_units(sample_data):
    """
    Test the histogram function with units conversion.
    """
    hist = histogram(sample_data, units='cm', bins=5, range=(0, 100))
    assert hist.center_of_bin.attrs['units'] == 'cm'

@pytest.mark.aqua
def test_histogram_type_error():
    """
    Test that a TypeError is raised for invalid input data.
    """
    with pytest.raises(TypeError):
        histogram(np.random.rand(10, 10))

@pytest.mark.aqua
def test_histogram_dask(sample_dask_data):
    """
    Test the histogram function with a dask array.
    """
    hist = histogram(sample_dask_data, bins=5, check=True, range=(0, 1), weighted=False)
    assert isinstance(hist, xr.DataArray)
    assert hist.name == 'histogram'
    assert 'center_of_bin' in hist.coords
    # The result of histogram with dask is a dask array, so we need to compute it
    assert int(hist.sum().compute()) == sample_dask_data.size

@pytest.mark.aqua
def test_histogram_dataset(sample_dataset):
    """
    Test the histogram function with an xarray.Dataset.
    """
    hist = histogram(sample_dataset, bins=5, check=True, range=(0, 1), weighted=False)
    assert isinstance(hist, xr.DataArray)
    assert hist.name == 'histogram'
    assert 'center_of_bin' in hist.coords
    assert int(hist.sum()) == sample_dataset['test_data'].size

@pytest.mark.aqua
def test_histogram_weighted_no_lat():
    """
    Test that a ValueError is raised when weighted=True and 'lat' coordinate is missing.
    """
    data = xr.DataArray(np.random.rand(10, 10), dims=['x', 'y'], name='test_data')
    data.attrs['units'] = 'm'
    data.attrs['long_name'] = 'Test Data'
    with pytest.raises(ValueError):
        histogram(data, weighted=True)

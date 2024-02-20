import pytest
import xarray as xr
import numpy as np
from pypdf import PdfReader

from aqua import Reader
from aqua.util.graphics import add_cyclic_lon, plot_box, minmax_maps
from aqua.util import cbar_get_label, evaluate_colorbar_limits, add_pdf_metadata
from aqua.graphics import plot_single_map

loglevel = 'DEBUG'


@pytest.fixture()
def da():
    """Create a test DataArray"""
    lon_values = np.linspace(0, 360, 36)  # Longitude values
    lat_values = np.linspace(-90, 90, 18)  # Latitude values
    data = np.random.rand(18, 36)  # Example data
    return xr.DataArray(data, dims=['lat', 'lon'], coords={'lon': lon_values, 'lat': lat_values})


@pytest.mark.graphics
def test_add_cyclic_lon(da):
    """Test the add_cyclic_lon function"""
    old_da = da.copy()
    new_da = add_cyclic_lon(da)

    # Assertions to test the function
    assert isinstance(new_da, xr.DataArray), "Output should be an xarray.DataArray"
    assert 'lon' in new_da.coords, "Output should have a 'lon' coordinate"
    assert 'lat' in new_da.coords, "Output should have a 'lat' coordinate"
    assert np.allclose(new_da.lat, old_da.lat), "Latitude values should be equal"
    assert np.allclose(new_da.isel(lon=-1).values, old_da.isel(lon=0).values), \
           "First and last longitude values should be equal"
    assert new_da.shape == (18, 37), "Output shape is incorrect"


@pytest.mark.graphics
def test_plot_box():
    """Test the plot box function"""
    num_rows, num_cols = plot_box(10)
    assert num_rows == 3, "Number of rows should be 3"
    assert num_cols == 4, "Number of columns should be 4"

    num_rows, num_cols = plot_box(1)
    assert num_rows == 1, "Number of rows should be 1"
    assert num_cols == 1, "Number of columns should be 1"

    num_rows, num_cols = plot_box(3)
    assert num_rows == 2, "Number of rows should be 2"
    assert num_cols == 2, "Number of columns should be 2"

    with pytest.raises(ValueError):
        plot_box(0)


@pytest.mark.graphics
def test_minmax_maps(da):
    """Test the minmax_maps function"""
    # Create a list of DataArrays
    maps = [da, da + 1, da + 2]

    # Test the function
    min_val, max_val = minmax_maps(maps)

    assert min_val < max_val, "Minimum value should be less than maximum value"
    for i in range(len(maps)):
        assert min_val <= maps[i].min().values, "Minimum value should be less than minimum value of the map"
        assert max_val >= maps[i].max().values, "Maximum value should be greater than maximum value of the map"


@pytest.mark.graphics
def test_label():
    """Test the cbar_get_label function"""
    # Retrieve data
    reader = Reader(model='IFS', exp='test-tco79', source='short', loglevel=loglevel)
    ds = reader.retrieve()
    da = ds['2t']

    # Test cbar_get_label function
    label = cbar_get_label(da, loglevel=loglevel)
    # assert label is a string
    assert isinstance(label, str), "Colorbar label should be a string"
    assert label == '2t [K]', "Colorbar label is incorrect"

    # Test the function with a custom label
    label = cbar_get_label(da, cbar_label='Temperature', loglevel=loglevel)
    assert label == 'Temperature', "Colorbar label is incorrect"

    # Test the cbar limits function with sym=False
    vmin, vmax = evaluate_colorbar_limits(da, sym=False)
    assert vmin < vmax, "Minimum value should be less than maximum value"
    assert vmin == 232.79393005371094, "Minimum value is incorrect"
    assert vmax == 310.61033630371094, "Maximum value is incorrect"

    # Test the cbar limits function with sym=True
    vmin, vmax = evaluate_colorbar_limits(da, sym=True)
    assert vmin < vmax, "Minimum value should be less than maximum value"
    assert vmin == -310.61033630371094, "Minimum value is incorrect"
    assert vmax == 310.61033630371094, "Maximum value is incorrect"


@pytest.mark.graphics
def test_pdf_metadata():
    """Test the add_pdf_metadata function"""
    # Generate a test figure from a random xarray DataArray
    da = xr.DataArray(np.random.rand(18, 36), dims=['lat', 'lon'], coords={'lon': np.linspace(0, 360, 36),
                                                                          'lat': np.linspace(-90, 90, 18)})
    plot_single_map(da, title='Test', save=True, filename='test', format='pdf', loglevel=loglevel)

    # Test the function
    add_pdf_metadata(filename='./test.pdf', metadata_value='Test',
                     metadata_name='/Test description', loglevel=loglevel)
    add_pdf_metadata(filename='./test.pdf', metadata_value='Test caption',
                     loglevel=loglevel)

    # Open the PDF and check the metadata
    pdf_reader = PdfReader("./test.pdf")
    metadata = pdf_reader.metadata

    assert metadata['/Test description'] == 'Test', "Old metadata should be kept"
    assert metadata['/Description'] == 'Test caption', "Description should be added to metadata"

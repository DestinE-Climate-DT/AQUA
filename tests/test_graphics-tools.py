import pytest
import xarray as xr
import numpy as np
import healpy as hp
from scipy.interpolate import griddata
from pypdf import PdfReader

from aqua import Reader
from aqua.core.util.graphics import add_cyclic_lon, plot_box, minmax_maps
from aqua.core.util import cbar_get_label, evaluate_colorbar_limits, add_pdf_metadata
from aqua.core.util import get_nside, get_npix, healpix_resample
from aqua.core.util import coord_names, set_map_title
from aqua.core.graphics import plot_single_map
from conftest import DPI, LOGLEVEL

loglevel = LOGLEVEL


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

    with pytest.raises(ValueError):
        add_cyclic_lon(da='test')  # Test with invalid input


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

@pytest.fixture(scope='module')
def ifs_data():
    """Retrieve IFS data for graphics tools tests"""
    reader = Reader(model='IFS', exp='test-tco79', source='short', loglevel=loglevel)
    return reader.retrieve()

@pytest.mark.graphics
def test_label(ifs_data):
    """Test the cbar_get_label function"""
    da = ifs_data['2t']

    # Test cbar_get_label function
    label = cbar_get_label(da, loglevel=loglevel)
    # assert label is a string
    assert isinstance(label, str), "Colorbar label should be a string"
    assert label == "2 metre temperature [K]", "Colorbar label is incorrect"

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

    with pytest.raises(ValueError):
        evaluate_colorbar_limits(maps=None)


@pytest.mark.graphics
def test_pdf_metadata(tmp_path):
    """Test the add_pdf_metadata function"""
    # Generate a test figure from a random xarray DataArray
    da = xr.DataArray(np.random.rand(18, 36), dims=['lat', 'lon'], coords={'lon': np.linspace(0, 360, 36),
                                                                           'lat': np.linspace(-90, 90, 18)})
    fig, _ = plot_single_map(da, title='Test', filename='test', format='pdf',
                             return_fig=True, loglevel=loglevel)

    fig.savefig(tmp_path / 'test.pdf', dpi=DPI)
    filename = str(tmp_path / 'test.pdf')
    # Test the function
    add_pdf_metadata(filename=filename, metadata_value='Test',
                     metadata_name='/Test description', loglevel=loglevel)
    add_pdf_metadata(filename=filename, metadata_value='Test caption',
                     loglevel=loglevel)

    # Open the PDF and check the metadata
    pdf_reader = PdfReader(filename)
    metadata = pdf_reader.metadata

    assert metadata['/Test description'] == 'Test', "Old metadata should be kept"
    assert metadata['/Description'] == 'Test caption', "Description should be added to metadata"


@pytest.mark.graphics
def test_set_map_title(da):
    title = set_map_title(da)

    assert title is None, "Title should be None"


@pytest.mark.graphics
def test_coord_names():
    """Test the coord_names function"""
    # Create a test DataArray
    lon_values = np.linspace(0, 360, 36)
    lat_values = np.linspace(-90, 90, 18)
    data = np.random.rand(18, 36)
    da = xr.DataArray(data, dims=['latitude', 'longitude'],
                      coords={'longitude': lon_values, 'latitude': lat_values})

    # Test the function
    lon_name, lat_name = coord_names(da)
    assert lon_name == 'longitude', "Longitude name is incorrect"
    assert lat_name == 'latitude', "Latitude name is incorrect"

@pytest.mark.graphics
class TestHealpixUtils:
    def test_get_nside_valid_ndarray(self):
        nside = 8
        npix = hp.nside2npix(nside)
        data = np.arange(npix)
        assert get_nside(data) == nside

    def test_get_nside_valid_xarray(self):
        nside = 4
        npix = hp.nside2npix(nside)
        data = xr.DataArray(np.arange(npix))
        assert get_nside(data) == nside

    def test_get_nside_invalid_type(self):
        with pytest.raises(ValueError, match="Input data must be a numpy array or xarray DataArray"):
            get_nside("invalid")

    def test_get_nside_empty_array(self):
        with pytest.raises(ValueError, match="data array is empty"):
            get_nside(np.array([]))

    def test_get_nside_invalid_npix(self):
        data = np.arange(123)  # 123 not a valid npix
        with pytest.raises(ValueError, match="Invalid HEALPix map: npix=123"):
            get_nside(data)

    def test_get_npix_valid(self):
        nside = 16
        data = np.arange(hp.nside2npix(nside))
        expected = hp.nside2npix(nside)
        assert get_npix(data) == expected

    def test_get_npix_invalid_type(self):
        with pytest.raises(ValueError, match="Input data must be a numpy array or xarray DataArray"):
            get_npix("invalid")

    def test_get_npix_invalid_npix(self):
        data = np.arange(999)  # Not valid npix
        with pytest.raises(ValueError, match="Invalid HEALPix map: npix=999"):
            get_npix(data)

    def test_get_npix_empty_array(self):
        with pytest.raises(ValueError, match="data array is empty"):
            get_npix(np.array([]))


@pytest.fixture
def healpix_data():
    nside = 8
    npix = hp.nside2npix(nside)
    data = xr.DataArray(np.random.rand(npix))
    xlims = (-30, 30)
    ylims = (-30, 30)
    return data, xlims, ylims

@pytest.mark.graphics
class TestHealpixResample:
    def test_healpix_resample_nearest(self, healpix_data):
        data, xlims, ylims = healpix_data
        result = healpix_resample(data, xlims=xlims, ylims=ylims, nx=10, ny=10, method="nearest")
        assert isinstance(result, xr.DataArray)
        assert result.ndim == 2
        assert result.shape == (10, 10)

    def test_healpix_resample_linear(self, healpix_data):
        data, xlims, ylims = healpix_data
        result = healpix_resample(data, xlims=xlims, ylims=ylims, nx=10, ny=10, method="linear")
        assert isinstance(result, xr.DataArray)
        assert result.ndim == 2
        assert result.shape == (10, 10)

    def test_healpix_resample_default_grid_size(self, healpix_data):
        data, _, _ = healpix_data
        result = healpix_resample(data, method="nearest")
        assert isinstance(result, xr.DataArray)
        assert result.ndim == 2
        assert result.shape[0] > 0 and result.shape[1] > 0

    def test_healpix_resample_sparse_valid(self):
        nside = 8
        full_npix = hp.nside2npix(nside)  # 768, valid
        selected_cells = np.random.choice(full_npix, size=100, replace=False)
        
        # Build a *sparse* DataArray, but with npix = 768 (valid)
        # Set all values to NaN, except for the selected_cells
        data_array = np.full(full_npix, np.nan, dtype=np.float32)
        values = np.random.rand(100).astype(np.float32)
        data_array[selected_cells] = values
        
        # Assign the 'cell' coordinate
        var = xr.DataArray(data_array, coords={"cell": np.arange(full_npix)}, dims=["cell"])

        result = healpix_resample(var, nx=20, ny=20, method="nearest")
        assert isinstance(result, xr.DataArray)
        assert result.ndim == 2
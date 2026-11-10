import cartopy.crs as ccrs
import matplotlib.pyplot as plt
import numpy as np
import pytest
import xarray as xr
from astropy_healpix import healpy as hp
from utils_tests import LOGLEVEL

from aqua.core.util import (
    cbar_get_label,
    coord_names,
    evaluate_colorbar_limits,
    get_npix,
    get_nside,
    healpix_resample,
    set_map_title,
)
from aqua.core.util.graphics import (
    add_contour_labels,
    add_cyclic_lon,
    get_decimals,
    minmax_maps,
    plot_box,
    prettify_levels,
)


@pytest.fixture()
def da():
    """Create a test DataArray"""
    lon_values = np.linspace(0, 360, 36)  # Longitude values
    lat_values = np.linspace(-90, 90, 18)  # Latitude values
    data = np.arange(18 * 36.0).reshape(18, 36)
    return xr.DataArray(data, dims=["lat", "lon"], coords={"lon": lon_values, "lat": lat_values})


@pytest.mark.graphics
def test_add_cyclic_lon(da):
    """Test the add_cyclic_lon function"""
    old_da = da.copy()
    new_da = add_cyclic_lon(da)

    # Assertions to test the function
    assert isinstance(new_da, xr.DataArray), "Output should be an xarray.DataArray"
    assert "lon" in new_da.coords, "Output should have a 'lon' coordinate"
    assert "lat" in new_da.coords, "Output should have a 'lat' coordinate"
    assert new_da.lat.values == pytest.approx(old_da.lat.values), "Latitude values should be equal"
    assert new_da.isel(lon=-1).values == pytest.approx(old_da.isel(lon=0).values), (
        "First and last longitude values should be equal"
    )
    assert new_da.shape == (18, 37), "Output shape is incorrect"

    with pytest.raises(ValueError):
        add_cyclic_lon(da="test")  # Test with invalid input


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
@pytest.mark.parametrize(
    "values, expected",
    [
        pytest.param([[-3.0, 1.0], [2.0, 4.0], [0.0, 5.0]], (-5.0, 5.0), id="max_wins"),
        pytest.param([[-7.0, 1.0], [2.0, 4.0], [0.0, 5.0]], (-7.0, 7.0), id="min_wins"),
    ],
)
def test_minmax_maps(values, expected):
    """Limits are symmetric around 0 over all the maps, skipping None: fails if a map is ignored or limits are asymmetric."""
    maps = [xr.DataArray(value) for value in values]
    assert minmax_maps([maps[0], None, *maps[1:]]) == expected


@pytest.fixture(scope="module")
def ifs_data(ifs_tco79_short_reader):
    """Retrieve IFS data for graphics tools tests"""
    return ifs_tco79_short_reader.retrieve()


@pytest.mark.graphics
def test_label(ifs_data):
    """Test the cbar_get_label function"""
    da = ifs_data["2t"]

    # Test cbar_get_label function
    label = cbar_get_label(da, loglevel=LOGLEVEL)
    # assert label is a string
    assert isinstance(label, str), "Colorbar label should be a string"
    assert label == "2 metre temperature [K]", "Colorbar label is incorrect"

    # Test the function with a custom label
    label = cbar_get_label(da, cbar_label="Temperature", loglevel=LOGLEVEL)
    assert label == "Temperature", "Colorbar label is incorrect"

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
@pytest.mark.parametrize(
    "kwargs, expected",
    [
        ({"title": "My title"}, "My title"),
        ({}, "2 metre temperature IFS test-tco79"),
        ({"put_units": True}, "2 metre temperature [K] IFS test-tco79"),
        ({"put_units": True, "set_units": "m s-1"}, "2 metre temperature [m s$^{-1}$] IFS test-tco79"),
        ({"use_attr_name": "short_name"}, "2t IFS test-tco79"),
        ({"use_attr_name": "missing"}, "2 metre temperature IFS test-tco79"),
        ({"put_model_name": False, "put_exp_name": False}, "2 metre temperature"),
        ({"skip_varname": True}, " IFS test-tco79"),
    ],
)
def test_set_map_title(kwargs, expected):
    """The title is built from the variable name, units, model and experiment: fails if a part or an option is lost."""
    attrs = {
        "long_name": "2 metre temperature",
        "short_name": "2t",
        "units": "K",
        "AQUA_model": "IFS",
        "AQUA_exp": "test-tco79",
    }
    data = xr.DataArray(np.zeros((2, 2)), dims=["lat", "lon"], attrs=attrs)
    assert set_map_title(data, loglevel=LOGLEVEL, **kwargs) == expected


@pytest.mark.graphics
def test_set_map_title_fallbacks(da):
    """Without long_name the short_name is used; without any attribute there is no title."""
    assert set_map_title(da.assign_attrs(short_name="2t"), loglevel=LOGLEVEL) == "2t"
    assert set_map_title(da, loglevel=LOGLEVEL) is None


@pytest.mark.graphics
def test_coord_names():
    """Test the coord_names function"""
    # Create a test DataArray
    lon_values = np.linspace(0, 360, 36)
    lat_values = np.linspace(-90, 90, 18)
    data = np.zeros((18, 36))
    da = xr.DataArray(data, dims=["latitude", "longitude"], coords={"longitude": lon_values, "latitude": lat_values})

    # Test the function
    lon_name, lat_name = coord_names(da)
    assert lon_name == "longitude", "Longitude name is incorrect"
    assert lat_name == "latitude", "Latitude name is incorrect"


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
        """Without limits the whole globe is resampled at 58.6 / nside degrees: (180, 360) / 7.325 = (24, 49) points."""
        data, _, _ = healpix_data
        result = healpix_resample(data, method="nearest")
        assert result.shape == (24, 49)

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


@pytest.mark.graphics
class TestPrettifyLevels:
    @pytest.mark.parametrize(
        "vmin, vmax, n, expected",
        [
            (0, 100, 10, [0, 10, 20, 30, 40, 50, 60, 70, 80, 90, 100]),
            (0, 0.05, 5, [0, 0.01, 0.02, 0.03, 0.04, 0.05]),
            (0, 1e5, 4, [0, 25000, 50000, 75000, 100000]),
            (-50, 50, 10, [-50, -40, -30, -20, -10, 0, 10, 20, 30, 40, 50]),
            (0.3, 1.7, 3, [0, 0.5, 1, 1.5]),
            (0, 9.999, 0.99, [0]),
        ],
    )
    def test_prettify_levels(self, vmin, vmax, n, expected):
        """Levels are multiples of a nice step (1, 2, 2.5, 5, 10 times a power of 10) from below vmin up to vmax."""
        assert prettify_levels(vmin, vmax, n).tolist() == pytest.approx(expected)

    def test_prettify_levels_quarter_step(self):
        """A step of 0.25 keeps two decimals (levels must be equally spaced, #3376)."""
        assert prettify_levels(0, 1, 4).tolist() == pytest.approx([0, 0.25, 0.5, 0.75, 1])

    def test_prettify_levels_zero_range(self):
        assert prettify_levels(5.0, 5.0, 10).tolist() == [5.0]

    @pytest.mark.parametrize("step,expected", [(1, 0), (0.5, 1), (0.05, 2), (0.001, 3), (0.99, 1)])
    def test_get_decimals(self, step, expected):
        assert get_decimals(step) == expected


@pytest.mark.graphics
def test_add_contour_labels_fallback(monkeypatch):
    """If matplotlib fails to place the labels, the contour lines are kept without labels"""
    ax = plt.axes(projection=ccrs.Robinson())
    lon, lat = np.linspace(0, 360, 37), np.linspace(-90, 90, 19)
    data = np.cos(np.deg2rad(lat))[:, None] * np.sin(np.deg2rad(lon))
    cs = ax.contour(lon, lat, data, levels=[-0.5, 0.5], transform=ccrs.PlateCarree())
    paths, transform = list(cs.get_paths()), cs.get_transform()

    # Fail after a first label has been placed, as the matplotlib bug does
    split, calls = cs._split_path_and_get_label_rotation, []

    def failing_split(*args, **kwargs):
        calls.append(1)
        if len(calls) > 1:
            raise IndexError("index -1 is out of bounds for axis 0 with size 0")
        return split(*args, **kwargs)

    monkeypatch.setattr(cs, "_split_path_and_get_label_rotation", failing_split)
    add_contour_labels(cs, loglevel=LOGLEVEL)

    assert not cs.labelTexts and not ax.texts
    assert cs.get_paths() == paths and cs.get_transform() is transform
    plt.close()

import dask.array as da
import numpy as np
import pytest
import xarray as xr
from utils_tests import LOGLEVEL

from aqua import histogram

pytestmark = pytest.mark.aqua

# 5 bins over (0, 1): each bin holds two latitude rows of the sample data
BINS = 5
CENTERS = [0.1, 0.3, 0.5, 0.7, 0.9]


@pytest.fixture
def sample_data():
    """A 10x10 field with values (10*lat_index + lon_index + 0.5) / 100: 100 distinct values in (0, 1), none on a bin edge.

    With 5 bins over (0, 1) every bin holds 20 values, which come from the latitude rows 2k and 2k+1.
    """
    lats = np.arange(0, 10, 1)
    lons = np.arange(0, 10, 1)
    values = (10 * lats[:, None] + lons[None, :] + 0.5) / 100
    data = xr.DataArray(values, coords=[("lat", lats), ("lon", lons)], name="test_data")
    data.attrs["units"] = "m"
    data.attrs["long_name"] = "Test Data"
    return data


def test_histogram_basic(sample_data):
    """Counts, bin centers, widths and attributes of a plain histogram: fails on wrong counts, bins or metadata."""
    hist = histogram(sample_data, bins=BINS, check=True, range=(0, 1), weighted=False, loglevel=LOGLEVEL)

    assert hist.name == "histogram"
    assert hist.values.tolist() == [20] * BINS
    assert hist.center_of_bin.values == pytest.approx(CENTERS)
    assert hist.width.values == pytest.approx([0.2] * BINS)
    assert hist.center_of_bin.attrs["units"] == "m"
    assert hist.attrs["units"] == "counts"
    assert hist.attrs["long_name"] == "Histogram of Test Data"
    assert hist.attrs["size_of_the_data"] == 100


def test_histogram_density(sample_data):
    """A uniform field gives a pdf equal to 1 in every bin: fails if the density is not normalised by the bin width."""
    hist = histogram(sample_data, bins=BINS, density=True, range=(0, 1), weighted=False, loglevel=LOGLEVEL)

    assert hist.name == "pdf"
    assert hist.values == pytest.approx([1.0] * BINS)
    assert hist.attrs["units"] == "probability density"
    assert hist.attrs["long_name"] == "Pdf of Test Data"


def test_histogram_weighted(sample_data):
    """Latitude weights are cos(lat): bin k holds 10 values from each of the rows 2k and 2k+1."""
    hist = histogram(sample_data, bins=BINS, weighted=True, check=True, range=(0, 1), loglevel=LOGLEVEL)

    rows = np.arange(10).reshape(BINS, 2)
    expected = 10 * np.cos(np.radians(rows)).sum(axis=1)
    assert hist.values == pytest.approx(expected)


def test_histogram_weights(sample_data):
    """Explicit weights (latitude index + 1) are used instead of cos(lat): bin k holds 10 * ((2k+1) + (2k+2))."""
    weights = xr.ones_like(sample_data) * (sample_data.lat + 1)
    hist = histogram(sample_data, bins=BINS, weights=weights, check=True, range=(0, 1), loglevel=LOGLEVEL)

    assert hist.values == pytest.approx([30, 70, 110, 150, 190])


def test_histogram_weights_size_mismatch(sample_data):
    """Weights with a different size than the data are an error"""
    weights = xr.DataArray(np.ones(10), dims=["lat"])
    with pytest.raises(ValueError, match="Weights must have the same size as the data"):
        histogram(sample_data, weights=weights, range=(0, 1), loglevel=LOGLEVEL)


def test_histogram_units(sample_data):
    """Data are converted from m to cm before binning: fails if only the units attribute changes."""
    hist = histogram(sample_data, units="cm", bins=BINS, range=(0, 100), weighted=False, loglevel=LOGLEVEL)

    assert hist.values.tolist() == [20] * BINS
    assert hist.center_of_bin.values == pytest.approx([100 * center for center in CENTERS])
    assert hist.center_of_bin.attrs["units"] == "cm"


def test_histogram_type_error():
    """A numpy array is not accepted"""
    with pytest.raises(TypeError, match="Input data must be an xarray DataArray or Dataset"):
        histogram(np.zeros((10, 10)))


@pytest.mark.parametrize("check", [False, True])
def test_histogram_dask(sample_data, check):
    """With dask data the histogram stays lazy unless check=True computes it, and gives the numpy counts."""
    data = sample_data.chunk({"lat": 5, "lon": 5})
    hist = histogram(data, bins=BINS, range=(0, 1), weighted=False, check=check, loglevel=LOGLEVEL)

    assert isinstance(hist.data, da.Array) is not check
    assert hist.compute().values.tolist() == [20] * BINS


def test_histogram_dataset(sample_data):
    """The first variable of a Dataset is used: the second variable has all its values in the first bin."""
    ds = sample_data.to_dataset()
    ds["second_var"] = xr.full_like(sample_data, 0.05)
    hist = histogram(ds, bins=BINS, check=True, range=(0, 1), weighted=False, loglevel=LOGLEVEL)

    assert hist.values.tolist() == [20] * BINS
    assert hist.attrs["long_name"] == "Histogram of Test Data"


def test_histogram_weighted_no_lat():
    """Latitude weights need a 'lat' coordinate"""
    data = xr.DataArray(np.zeros((10, 10)), dims=["x", "y"], name="test_data", attrs={"units": "m"})
    with pytest.raises(ValueError, match="must have a 'lat' coordinate"):
        histogram(data, weighted=True)

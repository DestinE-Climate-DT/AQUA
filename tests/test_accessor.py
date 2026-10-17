"""Testing if the accessor works"""

import dask.array as da
import pytest
import xarray as xr
from dask.callbacks import Callback

from aqua import Reader
from aqua.core.util import convert_data_units, select_season


# Aliases with module scope for fixtures
@pytest.fixture(scope="module")
def reader_instance(ifs_tco79_short_r100_reader):
    return ifs_tco79_short_r100_reader


@pytest.fixture(scope="module")
def data(ifs_tco79_short_r100_data):
    return ifs_tco79_short_r100_data


@pytest.fixture(scope="module")
def reader_instance2(ifs_tco79_short_r200_reader):
    return ifs_tco79_short_r200_reader


@pytest.fixture(scope="module")
def data2(ifs_tco79_short_r200_data):
    return ifs_tco79_short_r200_data


# Test classes
@pytest.mark.aqua
class TestAccessor:
    """Test class for accessor"""

    def test_accessor_fldmean(self, data):
        """Test fldmean as accessor"""
        avg = data.aqua.fldmean()["2t"].values
        assert avg[1] == pytest.approx(285.75920)

    def test_accessor_histogram(self, data):
        """Test histogram as accessor"""
        hist = data["2t"].aqua.histogram(bins=200, range=(150, 350), weighted=False)
        assert sum(hist.values) == data["2t"].size

    def test_accessor_two(self, reader_instance, reader_instance2, data, data2):
        """Test the use of two reader instances"""
        # test accessor on dataset, the object should remember the correct reader
        data1r = data.aqua.regrid()
        data2r = data2.aqua.regrid()

        assert len(data1r.lon) == 360
        assert len(data2r.lon) == 180

        # Check if the accessor still works with the result
        reader_instance2.set_default()  # An unrelated default must not change the result's Reader
        avg = data1r.aqua.fldmean()["2t"].values
        assert avg[1] == pytest.approx(285.7543, rel=1e-4)

        # Each regridded result keeps its own Reader without manual rebinding
        reader_instance.set_default()
        avg = data2r.aqua.fldmean()["2t"].values
        assert avg[1] == pytest.approx(285.7543, rel=1e-4)

        # Test accessor on dataarray (should be using the previous default)
        avgda = data2["2t"].aqua.fldmean().values
        assert avgda[1] == pytest.approx(285.7543, rel=1e-4)


@pytest.fixture(scope="module")
def binding_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", regrid="r100")


@pytest.mark.aqua
@pytest.mark.parametrize("as_dataarray", [False, True], ids=["dataset", "dataarray"])
@pytest.mark.parametrize("use_accessor", [False, True], ids=["reader", "accessor"])
@pytest.mark.parametrize(
    "operation", ["select_area", "select_area_regridded", "regrid", "seldate", "histogram", "timmean", "fldmean", "detrend"]
)
def test_transformed_result_keeps_reader(
    binding_reader, reader_instance2, ifs_tco79_long_data, as_dataarray, use_accessor, operation
):
    """Chained operations must use the originating Reader despite another global default."""
    field = ifs_tco79_long_data[["2t"]].isel(time=slice(0, 2)).copy()
    field.attrs["history"] = "AQUA-008 regression provenance"
    field["2t"].attrs["history"] = field.attrs["history"]
    if as_dataarray:
        field = field["2t"]
    if operation == "select_area_regridded":
        field = binding_reader.regrid(field)
    field.aqua.set_default(binding_reader)
    reader_instance2.set_default()

    method = "select_area" if operation == "select_area_regridded" else operation
    kwargs = {}
    if method == "select_area":
        kwargs = {"lon": [0, 60], "lat": [0, 60]}
    elif method == "seldate":
        kwargs = {"startdate": str(field.time.isel(time=0).values)}
    elif method == "histogram":
        kwargs = {"bins": 10, "range": (150, 350), "weighted": False}

    tasks = []
    with Callback(posttask=lambda *args: tasks.append(1)):
        result = getattr(field.aqua, method)(**kwargs) if use_accessor else getattr(binding_reader, method)(field, **kwargs)

    assert Reader.instance is reader_instance2
    assert result.aqua.instance is binding_reader
    assert not tasks
    values = result["2t"] if isinstance(result, xr.Dataset) else result
    assert isinstance(values.data, da.Array)
    assert "AQUA-008 regression provenance" in result.attrs["history"]

    if method == "histogram":
        expected_size = field["2t"].size if isinstance(field, xr.Dataset) else field.size
        assert result.sum().compute().item() == expected_size
    elif method == "fldmean":
        xr.testing.assert_allclose(result.compute(), binding_reader.fldmean(field).compute())
    else:
        expected = binding_reader.fldmean(result)
        assert Reader.instance is reader_instance2
        actual = result.aqua.fldmean()
        assert Reader.instance is reader_instance2
        assert actual.aqua.instance is binding_reader
        xr.testing.assert_allclose(actual.compute(), expected.compute())


@pytest.mark.aqua
@pytest.mark.parametrize("preprocessing", ["none", "identity", "slice", "other_reader"])
def test_retrieve_binds_preprocessed_result(reader_instance, reader_instance2, data, monkeypatch, preprocessing):
    """Preprocessing may return a new object already associated with another Reader."""
    field = data[["2t"]].isel(time=slice(0, 2))
    monkeypatch.setattr(reader_instance.backend, "retrieve", lambda **kwargs: field.copy())

    def preprocess(dataset):
        assert dataset.aqua.instance is reader_instance
        assert Reader.instance is reader_instance2
        if preprocessing == "identity":
            return dataset
        result = dataset.isel(time=slice(0, 1))
        if preprocessing == "other_reader":
            result.aqua.set_default(reader_instance2)
        return result

    monkeypatch.setattr(reader_instance, "preproc", None if preprocessing == "none" else preprocess)
    reader_instance2.set_default()
    result = reader_instance.retrieve(var="2t")
    assert Reader.instance is reader_instance2
    assert result.aqua.instance is reader_instance
    assert result.sizes["time"] == (2 if preprocessing in ("none", "identity") else 1)
    expected = reader_instance.fldmean(result)
    reader_instance2.set_default()
    xr.testing.assert_allclose(result.aqua.fldmean().compute(), expected.compute())


@pytest.mark.aqua
@pytest.mark.parametrize("as_dataarray", [False, True], ids=["dataset", "dataarray"])
def test_public_default_and_xarray_slicing(reader_instance, reader_instance2, data, as_dataarray):
    """Explicit defaults retain their behavior; raw xarray slices get fresh accessors."""
    field = data[["2t"]].isel(time=slice(0, 2)).copy()
    if as_dataarray:
        field = field["2t"]
    assert field.aqua.set_default(reader_instance) is field
    assert Reader.instance is reader_instance
    assert field.aqua.instance is reader_instance

    reader_instance2.set_default()
    assert Reader.instance is reader_instance2
    assert field.aqua.instance is reader_instance
    sliced = field.isel(time=slice(0, 1))
    assert sliced.aqua.instance is reader_instance2

    assert field.aqua.set_default(reader_instance2) is field
    assert field.aqua.instance is reader_instance2
    assert Reader.instance is reader_instance2


UTILITY_OPERATIONS = [
    "units",
    "units_scale",
    "units_noop",
    "season",
    "season_noop",
    "trend",
    "detrend",
    "coeffs",
    "coeffs_normalized",
]


def _utility_result(field, operation, trender):
    """Exercise standalone transforms without using Reader return wrappers."""
    if operation.startswith("units"):
        units = {"units": "degC", "units_scale": "mK", "units_noop": "K"}[operation]
        return convert_data_units(field, "2t", units)
    if operation.startswith("season"):
        return select_season(field, "annual" if operation == "season_noop" else "DJF")
    if operation == "coeffs_normalized":
        return trender.coeffs(field, normalize=True)
    return getattr(trender, operation)(field)


@pytest.mark.aqua
@pytest.mark.parametrize("as_dataarray", [False, True], ids=["dataset", "dataarray"])
@pytest.mark.parametrize("operation", UTILITY_OPERATIONS)
def test_utilities_preserve_input_reader(binding_reader, reader_instance2, ifs_tco79_long_data, as_dataarray, operation):
    """Utility results inherit the input binding, even when a different Reader is default."""
    field = ifs_tco79_long_data[["2t"]].isel(time=slice(0, 12)).copy()
    field.attrs["history"] = "AQUA-008 utility provenance"
    field["2t"].attrs["history"] = field.attrs["history"]
    if as_dataarray:
        field = field["2t"]
    field.aqua.set_default(binding_reader)
    reader_instance2.set_default()

    tasks = []
    with Callback(posttask=lambda *args: tasks.append(1)):
        result = _utility_result(field, operation, reader_instance2.trender)

    assert not tasks
    assert result.aqua.instance is binding_reader
    assert field.aqua.instance is binding_reader
    assert Reader.instance is reader_instance2
    assert "history" in result.attrs
    assert "AQUA-008 utility provenance" in field.attrs["history"]
    if operation.startswith(("units", "season")):
        assert "AQUA-008 utility provenance" in result.attrs["history"]
    values = result if as_dataarray else result["2t"]
    assert isinstance(values.data, da.Array)
    if operation.endswith("noop"):
        assert result is field

    original = field if as_dataarray else field["2t"]
    if operation.startswith("units"):
        assert original.attrs["units"] == "K"
        assert values.attrs["units"] == {"units": "degC", "units_scale": "mK", "units_noop": "K"}[operation]
        factor, offset = {"units": (1, -273.15), "units_scale": (1000, 0), "units_noop": (1, 0)}[operation]
        xr.testing.assert_allclose(values.compute(), (original * factor + offset).compute())
    elif operation.startswith("season"):
        months = original.time.dt.month
        expected = original if operation.endswith("noop") else original.sel(time=months.isin([12, 1, 2]))
        xr.testing.assert_allclose(values.compute(), expected.compute())
    expected_mean = binding_reader.fldmean(result)
    actual_mean = result.aqua.fldmean()
    assert actual_mean.aqua.instance is binding_reader
    assert Reader.instance is reader_instance2
    xr.testing.assert_allclose(actual_mean.compute(), expected_mean.compute())


@pytest.mark.aqua
@pytest.mark.parametrize("as_dataarray", [False, True], ids=["dataset", "dataarray"])
@pytest.mark.parametrize("operation", UTILITY_OPERATIONS)
def test_utilities_do_not_initialize_unbound_accessor(
    reader_instance2, ifs_tco79_long_data, as_dataarray, operation, monkeypatch
):
    """Plain xarray inputs must work without a Reader default or acquire an implicit binding."""
    field = ifs_tco79_long_data[["2t"]].isel(time=slice(0, 12)).copy()
    if as_dataarray:
        field = field["2t"]
    assert "aqua" not in getattr(field, "_cache", {})
    monkeypatch.delattr(Reader, "instance")
    result = _utility_result(field, operation, reader_instance2.trender)
    assert "aqua" not in getattr(field, "_cache", {})
    assert "aqua" not in getattr(result, "_cache", {})
    assert not hasattr(Reader, "instance")

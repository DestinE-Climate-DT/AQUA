"""Tests for aqua.core.intake_drivers.xarray: the AQUA-provided netcdf/zarr intake drivers."""

import importlib.util
import sys

import intake
import numpy as np
import pytest
import xarray as xr
from intake.source import get_plugin_class

from aqua.core.intake_drivers.xarray import IntakeNetCDFSource, IntakeZarrSource, install_intake_xarray_stub


@pytest.fixture
def sample_dataset():
    """A small in-memory dataset with a time axis."""
    return xr.Dataset(
        {"tas": (("time", "lat"), np.arange(12.0).reshape(4, 3))},
        coords={
            "time": xr.date_range("2020-01-01", periods=4, freq="D"),
            "lat": [0.0, 1.0, 2.0],
        },
    )


@pytest.mark.aqua
class TestDriverRegistration:
    """The AQUA sources own the intake netcdf/zarr driver names."""

    def test_netcdf_driver_registered(self):
        assert get_plugin_class("netcdf") is IntakeNetCDFSource

    def test_zarr_driver_registered(self):
        assert get_plugin_class("zarr") is IntakeZarrSource

    def test_intake_xarray_importable(self):
        # Real package or AQUA stub: legacy catalogs carrying a
        # "plugins: source: - module: intake_xarray" block need this import to succeed.
        import intake_xarray  # noqa: F401


@pytest.mark.aqua
class TestIntakeNetCDFSource:
    """Behaviour of the netcdf source built on the intake 2 readers."""

    def test_to_dask_single_file(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        source = IntakeNetCDFSource(str(path))
        data = source.to_dask()
        assert data["tas"].chunks is not None
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_read_eager(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        source = IntakeNetCDFSource(str(path))
        data = source.read()
        assert data["tas"].chunks is None
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_to_dask_glob(self, tmp_path, sample_dataset):
        sample_dataset.isel(time=slice(0, 2)).to_netcdf(tmp_path / "sample_a.nc")
        sample_dataset.isel(time=slice(2, 4)).to_netcdf(tmp_path / "sample_b.nc")
        source = IntakeNetCDFSource(str(tmp_path / "sample_*.nc"))
        data = source.to_dask()
        assert data.sizes["time"] == 4
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_engine_defaults_to_netcdf4(self):
        source = IntakeNetCDFSource("dummy.nc")
        assert source.reader.kwargs["engine"] == "netcdf4"

    def test_engine_override_from_xarray_kwargs(self):
        source = IntakeNetCDFSource("dummy.nc", xarray_kwargs={"engine": "h5netcdf"})
        assert source.reader.kwargs["engine"] == "h5netcdf"
        assert source.xarray_kwargs == {"engine": "h5netcdf"}

    def test_call_returns_self(self):
        source = IntakeNetCDFSource("dummy.nc")
        assert source() is source
        assert source.get() is source

    def test_data_and_metadata_exposed(self):
        urls = ["a.nc", "b.nc"]
        source = IntakeNetCDFSource(urls, metadata={"fixer_name": "amazing_fixer"})
        assert source.data.url == urls
        assert source.metadata["fixer_name"] == "amazing_fixer"

    def test_single_file_tolerates_mfdataset_kwargs(self, tmp_path, sample_dataset):
        # mfdataset-only kwargs (combine etc.) are common in AQUA catalogs and must
        # not break single-file reads, which xarray routes to xr.open_dataset
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        source = IntakeNetCDFSource(str(path), combine="by_coords")
        data = source.to_dask()
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_filtered_single_file_tolerates_mfdataset_kwargs(self, tmp_path, sample_dataset):
        # the backend narrows source.data.url between reads (glob expansion, date
        # filtering): a one-file leftover must still read with mfdataset-only kwargs
        sample_dataset.isel(time=slice(0, 2)).to_netcdf(tmp_path / "sample_a.nc")
        sample_dataset.isel(time=slice(2, 4)).to_netcdf(tmp_path / "sample_b.nc")
        source = IntakeNetCDFSource(str(tmp_path / "sample_*.nc"), combine="by_coords")
        source.data.url = [str(tmp_path / "sample_b.nc")]
        data = source.reader.read()
        assert data.sizes["time"] == 2


@pytest.mark.aqua
class TestIntakeZarrSource:
    """Behaviour of the zarr source built on the intake 2 readers."""

    def test_to_dask(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        source = IntakeZarrSource(str(store))
        data = source.to_dask()
        assert data["tas"].chunks is not None
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_chunks_honored(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        source = IntakeZarrSource(str(store), chunks={"time": 2})
        data = source.to_dask()
        assert data["tas"].chunks[0] == (2, 2)

    def test_data_and_metadata_exposed(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        source = IntakeZarrSource(str(store), metadata={"source_grid_name": "lon-lat"})
        assert source.data.url == str(store)
        assert source.metadata["source_grid_name"] == "lon-lat"

    def test_glob_multiple_stores(self, tmp_path, sample_dataset):
        # DROP-generated entries point to multiple stores through a glob urlpath
        sample_dataset.isel(time=slice(0, 2)).to_zarr(tmp_path / "sample_a.zarr")
        sample_dataset.isel(time=slice(2, 4)).to_zarr(tmp_path / "sample_b.zarr")
        source = IntakeZarrSource(str(tmp_path / "sample_*.zarr"))
        data = source.to_dask()
        assert data.sizes["time"] == 4
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_single_store_tolerates_mfdataset_kwargs(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        source = IntakeZarrSource(str(store), combine="by_coords")
        data = source.to_dask()
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])


@pytest.mark.aqua
class TestUseCftimeFolding:
    """The deprecated 'use_cftime' catalog kwarg is folded into a CFDatetimeCoder."""

    def test_use_cftime_folded_into_coder(self):
        source = IntakeNetCDFSource("dummy.nc", xarray_kwargs={"use_cftime": True})
        assert "use_cftime" not in source.reader.kwargs
        assert "use_cftime" not in source.xarray_kwargs
        coder = source.xarray_kwargs["decode_times"]
        assert isinstance(coder, xr.coders.CFDatetimeCoder)
        assert coder.use_cftime is True

    def test_to_dask_decodes_to_cftime(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        source = IntakeNetCDFSource(str(path), xarray_kwargs={"use_cftime": True})
        data = source.to_dask()
        # cftime decoding produces object-dtype time values, with no FutureWarning
        assert data.time.dtype == object


@pytest.mark.aqua
class TestYAMLCatalog:
    """A v1 YAML catalog resolves to the AQUA sources, even with intake-xarray installed."""

    def test_netcdf_entry_end_to_end(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        catfile = tmp_path / "catalog.yaml"
        catfile.write_text(
            "sources:\n"
            "  sample:\n"
            "    driver: netcdf\n"
            "    args:\n"
            f'      urlpath: "{path}"\n'
            "      xarray_kwargs:\n"
            "        decode_times: true\n"
            "    metadata:\n"
            "      fixer_name: amazing_fixer\n"
        )
        cat = intake.open_catalog(str(catfile))
        source = cat.sample()
        assert isinstance(source, IntakeNetCDFSource)
        assert source.metadata["fixer_name"] == "amazing_fixer"
        assert source.xarray_kwargs["decode_times"] is True
        data = source.to_dask()
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])


@pytest.mark.aqua
def test_stub_installation(monkeypatch):
    """When intake_xarray is absent, the stub provides the legacy module."""
    real_find_spec = importlib.util.find_spec
    saved = {k: sys.modules.pop(k) for k in list(sys.modules) if k == "intake_xarray" or k.startswith("intake_xarray.")}
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name, *args, **kwargs: None if name == "intake_xarray" else real_find_spec(name, *args, **kwargs),
    )
    try:
        assert install_intake_xarray_stub() is True
        import intake_xarray

        # the stub exposes the legacy intake-xarray class names
        assert intake_xarray.netcdf.NetCDFSource is IntakeNetCDFSource
        assert intake_xarray.xzarr.ZarrSource is IntakeZarrSource
        # a second call must be a no-op now that the stub is in place
        assert install_intake_xarray_stub() is False
    finally:
        for key in list(sys.modules):
            if key == "intake_xarray" or key.startswith("intake_xarray."):
                sys.modules.pop(key)
        sys.modules.update(saved)

"""Tests for aqua.core.intake_drivers.xarray: the AQUA-provided netcdf/zarr intake drivers."""

import importlib.util
import sys

import numpy as np
import pytest
import xarray as xr
from intake.source import get_plugin_class

from aqua.core.intake_drivers.xarray import (
    IntakeNetCDFSource,
    IntakeZarrSource,
    install_intake_xarray_stub,
    open_netcdf,
    open_zarr,
)


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
class TestIntakeDrivers:
    """The AQUA sources are registered as the intake netcdf/zarr drivers."""

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

    def test_data_url_exposed(self):
        urls = ["a.nc", "b.nc"]
        source = IntakeNetCDFSource(urls)
        assert source.data.url == urls


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


@pytest.mark.aqua
class TestUseCftimeFolding:
    """The deprecated 'use_cftime' catalog kwarg is folded into a CFDatetimeCoder."""

    def test_use_cftime_folded_into_coder(self):
        source = IntakeNetCDFSource("dummy.nc", xarray_kwargs={"decode_times": True, "use_cftime": True})
        assert "use_cftime" not in source.reader.kwargs
        coder = source.reader.kwargs["decode_times"]
        assert isinstance(coder, xr.coders.CFDatetimeCoder)
        assert coder.use_cftime is True
        # the raw catalog kwargs stay exposed for backend introspection
        assert source.xarray_kwargs["use_cftime"] is True

    def test_to_dask_decodes_to_cftime(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        source = IntakeNetCDFSource(str(path), xarray_kwargs={"use_cftime": True})
        data = source.to_dask()
        # cftime decoding produces object-dtype time values, with no FutureWarning
        assert data.time.dtype == object


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


@pytest.mark.aqua
class TestOpeners:
    """The functional openers, mirroring the fdb/icechunk driver layout."""

    def test_open_netcdf_single_file_eager(self, tmp_path, sample_dataset):
        path = tmp_path / "sample.nc"
        sample_dataset.to_netcdf(path)
        # mfdataset-only kwargs must be tolerated on the single-file path
        data = open_netcdf(str(path), combine="by_coords")
        assert data["tas"].chunks is None  # open_dataset: eager-capable
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_open_netcdf_glob(self, tmp_path, sample_dataset):
        sample_dataset.isel(time=slice(0, 2)).to_netcdf(tmp_path / "sample_a.nc")
        sample_dataset.isel(time=slice(2, 4)).to_netcdf(tmp_path / "sample_b.nc")
        data = open_netcdf(str(tmp_path / "sample_*.nc"))
        assert data.sizes["time"] == 4
        assert data["tas"].chunks is not None  # open_mfdataset: dask-backed

    def test_open_netcdf_missing(self, tmp_path):
        with pytest.raises(FileNotFoundError):
            open_netcdf(str(tmp_path / "nothing_*.nc"))

    def test_open_zarr_glob_multiple_stores(self, tmp_path, sample_dataset):
        # DROP-generated entries point to multiple stores through a glob urlpath
        sample_dataset.isel(time=slice(0, 2)).to_zarr(tmp_path / "sample_a.zarr")
        sample_dataset.isel(time=slice(2, 4)).to_zarr(tmp_path / "sample_b.zarr")
        data = open_zarr(str(tmp_path / "sample_*.zarr"))
        assert data.sizes["time"] == 4
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

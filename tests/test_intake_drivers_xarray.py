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


@pytest.fixture
def netcdf_tree(tmp_path, sample_dataset):
    """One standalone file holding the whole sample, plus the same sample split in two."""
    sample_dataset.to_netcdf(tmp_path / "one.nc")
    sample_dataset.isel(time=slice(0, 2)).to_netcdf(tmp_path / "part_a.nc")
    sample_dataset.isel(time=slice(2, 4)).to_netcdf(tmp_path / "part_b.nc")
    return tmp_path


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

    @pytest.mark.parametrize("urlkind", ["single_str", "single_list", "multi_list", "glob"])
    def test_read_is_lazy_and_complete(self, netcdf_tree, sample_dataset, urlkind):
        # this is the backend's own read path: it calls source.reader.read(), not to_dask().
        # intake sends a single url to xr.open_dataset and a list/glob to xr.open_mfdataset,
        # and only the latter forces chunks={} on its own, hence the driver default (#3064)
        urlpath = {
            "single_str": str(netcdf_tree / "one.nc"),
            "single_list": [str(netcdf_tree / "one.nc")],
            "multi_list": [str(netcdf_tree / "part_a.nc"), str(netcdf_tree / "part_b.nc")],
            "glob": str(netcdf_tree / "part_*.nc"),
        }[urlkind]
        source = IntakeNetCDFSource(urlpath)
        assert source.reader.kwargs["chunks"] == {}
        data = source.reader.read()
        assert data["tas"].chunks is not None
        assert data.sizes["time"] == 4
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    @pytest.mark.parametrize(
        "kwargs,time_chunks",
        [
            ({}, (4,)),  # driver default: lazy in a single chunk
            ({"chunks": {"time": 1}}, (1, 1, 1, 1)),  # catalog args (and Reader(chunks=...)) win
            ({"xarray_kwargs": {"chunks": {"time": 2}}}, (2, 2)),  # so does the xarray_kwargs block
            ({"chunks": None}, None),  # explicit opt-out from dask is honoured
        ],
        ids=["driver_default", "source_args", "xarray_kwargs", "explicit_none"],
    )
    def test_chunks_precedence(self, netcdf_tree, kwargs, time_chunks):
        # the default must never be added on top of an explicit chunks, or the reader
        # would receive it twice and raise TypeError (real case: climatedt-phase1 zonalmean)
        data = IntakeNetCDFSource(str(netcdf_tree / "one.nc"), **kwargs).reader.read()
        if time_chunks is None:
            assert data["tas"].chunks is None
        else:
            assert data["tas"].chunks[0] == time_chunks

    def test_access_modes(self, netcdf_tree, sample_dataset):
        # the ported intake-xarray API: to_dask() is lazy, read()/discover() are eager
        source = IntakeNetCDFSource(str(netcdf_tree / "one.nc"))
        assert source.to_dask()["tas"].chunks is not None
        eager = source.read()
        assert eager["tas"].chunks is None
        xr.testing.assert_allclose(eager["tas"], sample_dataset["tas"])

    def test_engine_defaults_to_netcdf4(self):
        source = IntakeNetCDFSource("dummy.nc")
        assert source.reader.kwargs["engine"] == "netcdf4"
        # the exposed dict is the effective one: what the backend re-reads with
        assert source.xarray_kwargs["engine"] == "netcdf4"

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

    def test_single_file_tolerates_mfdataset_kwargs(self, netcdf_tree, sample_dataset):
        # mfdataset-only kwargs (combine etc.) are common in AQUA catalogs and must
        # not break single-file reads, which xarray routes to xr.open_dataset
        source = IntakeNetCDFSource(str(netcdf_tree / "one.nc"), combine="by_coords")
        xr.testing.assert_allclose(source.reader.read()["tas"], sample_dataset["tas"])

    def test_filtered_single_file_tolerates_mfdataset_kwargs(self, netcdf_tree):
        # the backend narrows source.data.url between reads (glob expansion, date
        # filtering): a one-file leftover must still read with mfdataset-only kwargs
        source = IntakeNetCDFSource(str(netcdf_tree / "part_*.nc"), combine="by_coords")
        source.data.url = [str(netcdf_tree / "part_b.nc")]
        assert source.reader.read().sizes["time"] == 2


@pytest.mark.aqua
class TestIntakeZarrSource:
    """Behaviour of the zarr source built on the intake 2 readers."""

    def test_read_is_lazy(self, tmp_path, sample_dataset):
        # a single store takes the same xr.open_dataset branch as a single netcdf file,
        # so it needs the same chunks default to come back dask-backed (#3064)
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        data = IntakeZarrSource(str(store)).reader.read()
        assert data["tas"].chunks is not None
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_chunks_honored(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        data = IntakeZarrSource(str(store), chunks={"time": 2}).reader.read()
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
        data = IntakeZarrSource(str(tmp_path / "sample_*.zarr")).reader.read()
        assert data.sizes["time"] == 4
        xr.testing.assert_allclose(data["tas"], sample_dataset["tas"])

    def test_single_store_tolerates_mfdataset_kwargs(self, tmp_path, sample_dataset):
        store = tmp_path / "sample.zarr"
        sample_dataset.to_zarr(store)
        source = IntakeZarrSource(str(store), combine="by_coords")
        xr.testing.assert_allclose(source.reader.read()["tas"], sample_dataset["tas"])


@pytest.mark.aqua
class TestXarrayKwargsPassthrough:
    """Catalog xarray_kwargs are exposed verbatim; time decoding is handled by the backend."""

    def test_xarray_kwargs_exposed_verbatim(self):
        # use_cftime and other kwargs are passed through untouched: the backend
        # (BackendIntakeXarray._setup_xarray_kwargs) is what decides time decoding
        source = IntakeNetCDFSource("dummy.nc", xarray_kwargs={"use_cftime": True})
        assert source.xarray_kwargs["use_cftime"] is True
        assert source.reader.kwargs["use_cftime"] is True

    def test_read_time_kwargs_keep_the_driver_defaults(self, netcdf_tree):
        # the backend re-reads with those kwargs, and BaseReader.read() merges them on top
        # of the reader ones: the engine and chunks defaults must survive that merge
        source = IntakeNetCDFSource(str(netcdf_tree / "one.nc"))
        data = source.reader.read(decode_times=xr.coders.CFDatetimeCoder(time_unit="s"))
        assert data["tas"].chunks is not None


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

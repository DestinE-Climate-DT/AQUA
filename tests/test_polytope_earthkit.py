"""Earthkit Polytope tests independent of GSV, FDB libraries and remote credentials."""

from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, Mock

import cloudpickle
import dask.array as da
import numpy as np
import pandas as pd
import pytest
import xarray as xr

from aqua import Reader
from aqua.core.exceptions import NoDataError
from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.openers import earthkit_source, open_polytope
from aqua.core.intake_drivers.fdb.openers.earthkit_source import EarthkitPolytopeSource

pytestmark = pytest.mark.aqua


@pytest.fixture
def source_args():
    return {
        "request": {"param": [130], "levtype": "pl", "levelist": [1000, 900, 800]},
        "data_start_date": "20080101T1200",
        "data_end_date": "20080101T1200",
        "engine": "polytope-earthkit",
    }


@pytest.fixture
def earthkit_mock(monkeypatch):
    """Mock only the network/decoder boundary for lightweight planner tests."""
    fields = MagicMock()
    fields.__len__.return_value = 1
    fields.to_xarray.return_value = xr.Dataset(
        {"t": (("time", "level", "cell"), np.arange(6.0).reshape(1, 3, 2), {"GRIB_paramId": 130})},
        coords={
            "time": pd.date_range("2008-01-01T12:00", periods=1),
            "level": ("level", [800, 900, 1000], {"units": "hPa"}),
            "cell": [0, 1],
        },
        attrs={"history": "original history", "AQUA_test": "preserved"},
    )
    from_source = Mock(return_value=SimpleNamespace(to_fieldlist=Mock(return_value=fields)))
    monkeypatch.setattr(earthkit_source, "earthkit_data", SimpleNamespace(from_source=from_source))
    return from_source, fields


@pytest.mark.parametrize("bridge", [None, "lumi", "mn5"])
def test_intake_routing_and_lazy_retrieval(source_args, earthkit_mock, bridge):
    from_source, fields = earthkit_mock
    original = deepcopy(source_args)
    source = IntakeFDBSource(**source_args, databridge=bridge)
    from_source.assert_not_called()
    data = source.to_dask()
    assert isinstance(data.t.data, da.Array)
    assert from_source.call_count == 1  # Only the existing FDB schema probe runs now.
    args, kwargs = from_source.call_args
    assert args[:2] == ("polytope", "destination-earth")
    assert args[2]["date"] == "20080101"
    assert args[2]["time"] == "1200"
    assert kwargs == {
        "stream": False, "address": f"polytope.{bridge or 'lumi'}.apps.dte.destination-earth.eu", "log_level": "WARNING",
    }
    assert fields.to_xarray.call_args.kwargs["ensure_dims"] == ["valid_time", "level"]
    np.testing.assert_array_equal(data.level, [1000, 900, 800])
    np.testing.assert_array_equal(data.compute(scheduler="synchronous").t.data, [[[4, 5], [2, 3], [0, 1]]])
    assert from_source.call_count == 2
    assert "original history" in data.attrs["history"]
    assert "Earthkit Polytope" in data.isel(time=0).attrs["history"]
    assert data.attrs["AQUA_test"] == "preserved"
    assert source_args == original


def test_custom_service_and_serialization(source_args, earthkit_mock):
    from_source, _ = earthkit_mock
    metadata = {"polytope": {"address": "polytope.ecmwf.int", "collection": "ecmwf-mars"}}
    source = EarthkitPolytopeSource(**source_args, metadata=metadata, databridge="custom")
    data = source.to_dask()
    # Repeated schema access must use the cached sample, without testing DataArray truthiness.
    source.to_dask()
    assert from_source.call_count == 1
    assert from_source.call_args.args[1] == "ecmwf-mars"
    assert from_source.call_args.kwargs["address"] == "polytope.ecmwf.int"
    restored = cloudpickle.loads(cloudpickle.dumps(data))
    xr.testing.assert_equal(restored.compute(scheduler="synchronous"), data.compute(scheduler="synchronous"))


@pytest.mark.parametrize(
    "start,end,first,month",
    [
        ("19900101", "19900301", True, "1"),
        ("19900101", "19900301", False, "1/to/3"),
        ("19900901", "19900901", False, "9"),
        ("19901001", "19901201", False, "10/to/12"),
    ],
)
def test_monthly_polytope_request(earthkit_mock, start, end, first, month):
    """MARS month enums must be unpadded, including the Reader initialization probe."""
    from_source, _ = earthkit_mock
    source = EarthkitPolytopeSource(
        {"param": [235087], "levtype": "sfc", "stream": "clmn"},
        data_start_date=start, data_end_date=end,
        timestep="h", savefreq="MS", chunks="YS", timestyle="yearmonth",
    )
    source._get_partition(0, first=first)
    request = from_source.call_args.args[2]
    assert request["year"] == "1990"
    assert request["month"] == month


@pytest.mark.parametrize("chunk_key", ["level", "vertical"])
def test_level_selection_physical_values_and_chunking(source_args, earthkit_mock, chunk_key):
    from_source, fields = earthkit_mock
    full = fields.to_xarray.return_value

    def select_request(*args, **kwargs):
        fields.to_xarray.return_value = full.sel(level=np.atleast_1d(args[2]["levelist"]))
        return SimpleNamespace(to_fieldlist=lambda: fields)

    from_source.side_effect = select_request
    data = open_polytope(
        **source_args, metadata={"levels": [999.99, 899.99, 799.99]}, level=[900, 800], chunks={chunk_key: 1}
    )
    assert data.t.chunks == ((1,), (1, 1), (2,))
    np.testing.assert_array_equal(data.level, [899.99, 799.99])
    np.testing.assert_array_equal(data.compute(scheduler="synchronous").t.data, [[[2, 3], [0, 1]]])
    assert data.level.attrs["units"] == "hPa"


@pytest.mark.parametrize(
    "metadata,bridge,message",
    [
        ({}, "unknown", "Unknown databridge"),
        ({"polytope": "invalid"}, None, "must be a mapping"),
        ({"polytope": {"address": ""}}, None, "non-empty strings"),
        ({"polytope": {"collection": None}}, None, "non-empty strings"),
    ],
)
def test_invalid_service_configuration(source_args, earthkit_mock, metadata, bridge, message):
    with pytest.raises(ValueError, match=message):
        EarthkitPolytopeSource(**source_args, metadata=metadata, databridge=bridge)
    earthkit_mock[0].assert_not_called()


def test_missing_dependency(source_args, monkeypatch):
    monkeypatch.setattr(earthkit_source, "earthkit_data", None)
    with pytest.raises(ImportError, match="aqua-core\\[polytope\\]"):
        EarthkitPolytopeSource(**source_args)


def test_empty_response_and_service_errors(source_args, earthkit_mock):
    from_source, fields = earthkit_mock
    fields.__len__.return_value = 0
    with pytest.raises(NoDataError, match="no GRIB fields"):
        open_polytope(**source_args)
    from_source.side_effect = ConnectionError("service unavailable")
    with pytest.raises(ConnectionError, match="service unavailable"):
        open_polytope(**source_args)


def test_feature_requests_are_rejected(source_args, earthkit_mock):
    source_args["request"]["feature"] = {"type": "timeseries"}
    with pytest.raises(ValueError, match="full fields only"):
        open_polytope(**source_args)
    earthkit_mock[0].assert_not_called()


@pytest.fixture(scope="module")
def grib_fields():
    earthkit = pytest.importorskip("earthkit.data", minversion="1.3.1")
    path = Path(__file__).resolve().parents[1] / "AQUA_tests/fdb/sample_test_data.grib"
    if not path.is_file():
        pytest.skip("Existing AQUA GRIB sample is unavailable")
    return earthkit.from_source("file", str(path)).to_fieldlist()


@pytest.mark.parametrize("levelist", [[1000], [1000, 900, 800], ["1000"]])
def test_real_earthkit_decoder(source_args, grib_fields, monkeypatch, levelist):
    """Decode existing GRIB data through the real Earthkit backend, mocking the service only."""
    def retrieve(*args, **kwargs):
        selection = np.atleast_1d(args[2]["levelist"]).astype(int).tolist()
        return SimpleNamespace(to_fieldlist=lambda: grib_fields.sel({"metadata.level": selection}))

    monkeypatch.setattr(earthkit_source, "earthkit_data", SimpleNamespace(from_source=retrieve))
    source_args["request"]["levelist"] = levelist
    data = open_polytope(**source_args)
    assert isinstance(data.t.data, da.Array)
    assert data.sizes["time"] == 1
    assert data.sizes["level"] == len(levelist)
    np.testing.assert_array_equal(data.level, np.array(levelist, dtype=int))
    assert data.t.attrs["GRIB_paramId"] == 130
    assert data.t.attrs["units"] == "K"
    assert data.level.attrs["units"] == "hPa"
    data = cloudpickle.loads(cloudpickle.dumps(data))
    assert float(data.t.isel(level=0).mean().compute(scheduler="synchronous")) == pytest.approx(279.3509)


def test_reader_earthkit(grib_fields, monkeypatch):
    """Exercise the public Reader/catalog path, including DataModel and endpoint forwarding."""
    def retrieve(*args, **kwargs):
        assert kwargs["address"] == "polytope.mn5.apps.dte.destination-earth.eu"
        return SimpleNamespace(to_fieldlist=lambda: grib_fields.sel({"metadata.level": args[2]["levelist"]}))

    monkeypatch.setattr(earthkit_source, "earthkit_data", SimpleNamespace(from_source=retrieve))
    reader = Reader(
        catalog="ci", model="IFS", exp="test-fdb", source="fdb", engine="polytope-earthkit",
        databridge="mn5", fix=False, areas=False,
    )
    data = reader.retrieve(var=130)
    assert isinstance(data.t.data, da.Array)
    assert data.sizes["plev"] == 1
    assert data.plev.item() == 100000
    assert data.aqua.instance is reader
    assert float(data.t.mean().compute(scheduler="synchronous")) == pytest.approx(279.3509)


@pytest.mark.parametrize("chunks", ["h", "D"])
def test_real_healpix_and_time_chunks(monkeypatch, chunks):
    """Keep the original grid ordering, time samples and GRIB units on a real HEALPix field."""
    earthkit = pytest.importorskip("earthkit.data", minversion="1.3.1")
    path = Path(__file__).resolve().parents[1] / "AQUA_tests/fdb/sample_test_data_d1.grib"
    if not path.is_file():
        pytest.skip("Existing AQUA HEALPix GRIB sample is unavailable")
    fields = earthkit.from_source("file", str(path)).to_fieldlist()

    def retrieve(*args, **kwargs):
        request = args[2]
        times = request["time"].split("/to/")
        selection = list(range(int(times[0]), int(times[-1]) + 1, 100))
        return SimpleNamespace(
            to_fieldlist=lambda: fields.sel(
                {"metadata.date": int(request["date"].split("/to/")[0]), "metadata.time": selection}
            )
        )

    from_source = Mock(side_effect=retrieve)
    monkeypatch.setattr(earthkit_source, "earthkit_data", SimpleNamespace(from_source=from_source))
    data = open_polytope(
        {"param": [164], "levtype": "sfc"}, engine="polytope-earthkit", chunks=chunks,
        data_start_date="19900101", data_end_date="19900102",
    )
    assert from_source.call_count == 1
    assert data.sizes == {"time": 25, "values": 196608}
    assert data.tcc.chunks[0] == ((1,) * 25 if chunks == "h" else (24, 1))
    assert data.tcc.attrs["units"] == fields[0].metadata("units") == "(0 - 1)"
    assert data.latitude.dims == ("values",)
    assert data.longitude.dims == ("values",)
    np.testing.assert_array_equal(data.time, pd.date_range("1990-01-01", "1990-01-02", freq="h"))
    expected = fields[0].to_numpy().ravel()[:4]
    np.testing.assert_array_equal(data.tcc.isel(time=0, values=slice(4)).compute(scheduler="synchronous"), expected)

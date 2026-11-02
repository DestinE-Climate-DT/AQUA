"""Availability resolution and validation before z3fdb storage routing."""

from copy import deepcopy
from pathlib import Path
from unittest.mock import Mock, mock_open

import dask.array as da
import pandas as pd
import pytest
import requests
import xarray as xr

from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.datatypes import Z3FDB
from aqua.core.intake_drivers.fdb.openers import dates, timeutil
from aqua.core.intake_drivers.fdb.openers.dates import FDBDatesMixin
from aqua.core.intake_drivers.fdb.openers.gsv_source import GSVSource
from aqua.core.util import load_yaml

pytestmark = pytest.mark.aqua
INFO_FILE = Path(__file__).parent / "catgen" / "fdb_info_file.yaml"


@pytest.fixture
def opener(monkeypatch):
    """Record resolved requests without retrieving any field data."""
    result = Mock(
        side_effect=lambda request, **kwargs: xr.Dataset(
            {"var130": ("time", da.ones(1))}, coords={"time": [pd.Timestamp(kwargs["startdate"])]}
        )
    )
    monkeypatch.setattr("aqua.core.intake_drivers.fdb.readers.open_z3fdb", result)
    return result


def source(**kwargs):
    """Create an Intake source with three available days and separate FDB paths."""
    return IntakeFDBSource(
        **{
            "request": {"param": [130, 131], "expver": "0001"},
            "engine": "z3fdb",
            "data_start_date": "19900101T0000",
            "data_end_date": "19900103T2300",
            "metadata": {"fdb_path": "/hpc.yaml", "fdb_path_bridge": "/bridge.yaml"},
            **kwargs,
        }
    )


@pytest.mark.parametrize(
    "filename, expected_configs",
    [
        ("fdb_info_file.yaml", ["/bridge.yaml", "/hpc.yaml"]),
        ("fdb_info_hpc-only.yaml", ["/hpc.yaml"]),
    ],
)
def test_info_file_overrides_catalog(opener, filename, expected_configs):
    """Existing workflow fixtures override stale catalog dates and experiment IDs."""
    metadata = {
        "fdb_path": "/hpc.yaml",
        "fdb_path_bridge": "/bridge.yaml",
        "fdb_info_file": str(INFO_FILE.with_name(filename)),
    }
    request = {"param": [130], "expver": "0001"}
    original = deepcopy(metadata)
    catalog = source(request=request, metadata=metadata, data_start_date="20000101", data_end_date="20000102")
    result = catalog.to_dask()
    calls = opener.call_args_list
    assert [c.kwargs["config"] for c in calls] == expected_configs
    assert calls[0].kwargs["startdate"] == "19900101T0000"
    assert calls[-1].kwargs["enddate"] == "19900103T2300"
    assert all(c.args[0]["expver"] == "t039" for c in calls)
    assert all(c.kwargs["data_start_date"] == "19900101T0000" for c in calls)
    assert request == {"param": [130], "expver": "0001"}
    assert metadata == original
    assert isinstance(result.var130.data, da.Array)


def test_info_file_refreshes_dates_and_expvers(opener, monkeypatch):
    """Each read refreshes workflow dates and keeps HPC/bridge overrides separate."""
    first = load_yaml(str(INFO_FILE))
    first["data"]["expver"], first["bridge"]["expver"] = "a0h3", "0001"
    second = deepcopy(first)
    second["data"]["data_end_date"] = "19900104T2300"
    second["bridge"]["bridge_end_date"] = "19900103T2300"
    second["data"]["expver"], second["bridge"]["expver"] = "a0h4", "0002"
    read_info = Mock(side_effect=[first, second])
    monkeypatch.setattr(FDBDatesMixin, "get_fdb_definitions_from_file", read_info)
    catalog = source(metadata={"fdb_path": "/hpc.yaml", "fdb_path_bridge": "/bridge.yaml", "fdb_info_file": "info.yaml"})
    catalog.to_dask()
    catalog.to_dask()
    assert read_info.call_count == 2
    assert [c.args[0]["expver"] for c in opener.call_args_list] == ["0001", "a0h3", "0002", "a0h4"]
    assert [c.kwargs["enddate"] for c in opener.call_args_list] == [
        "19900102T2300",
        "19900103T2300",
        "19900103T2300",
        "19900104T2300",
    ]


@pytest.mark.parametrize(
    "config_fdb, metadata, expected_path",
    [
        (None, {"fdb_home": "/hpc"}, "/hpc/etc/fdb/config.yaml"),
        (None, {"fdb_path": "/hpc.yaml"}, "/hpc.yaml"),
        ("/override.yaml", {"fdb_home": "/unused"}, "/override.yaml"),
        ("/override", {"fdb_path": "/unused.yaml"}, "/override/etc/fdb/config.yaml"),
    ],
)
def test_auto_dates_use_config_and_hpc_expver(opener, monkeypatch, config_fdb, metadata, expected_path):
    """Use the shared directory scan, including its conservative last-day policy."""
    config = mock_open(read_data="spaces: [{roots: [{path: /fdb-root}]}]")
    monkeypatch.setattr(dates, "open", config, raising=False)
    listdir = Mock(
        return_value=[f"d1:climate-dt:cmip6:hist:1:ifs-nemo:1:a0h3:clte:{d}" for d in [19900103, 19900101, 19900102]]
        + ["d1:climate-dt:cmip6:hist:1:ifs-nemo:1:0001:clte:19910101"]
    )
    monkeypatch.setattr(dates.os, "listdir", listdir)
    request = dict(
        param=[130],
        expver="0001",
        **{
            "class": "d1",
            "dataset": "climate-dt",
            "activity": "CMIP6",
            "experiment": "hist",
            "generation": 1,
            "model": "IFS-NEMO",
            "realization": 1,
            "stream": "clte",
        },
    )
    catalog = source(
        request=request,
        config_fdb=config_fdb,
        metadata=metadata,
        hpc_expver="a0h3",
        data_start_date="auto",
        data_end_date="auto",
    )
    catalog.to_dask()
    config.assert_called_once_with(expected_path, "r")
    listdir.assert_called_once_with("/fdb-root")
    call = opener.call_args
    assert call.kwargs["startdate"] == "19900101T0000"
    assert call.kwargs["enddate"] == "19900102T2300"
    assert call.args[0]["expver"] == "a0h3"
    assert request["expver"] == "0001"


def test_yearmonth_auto_rejected_before_retrieval(opener):
    """Preserve GSV's explicit restriction on automatic monthly date discovery."""
    with pytest.raises(ValueError, match="Auto date selection not supported for timestyle=yearmonth"):
        source(data_end_date="auto", timestyle="yearmonth", savefreq="MS").to_dask()
    opener.assert_not_called()


def test_stac_dates_preserve_request(opener, monkeypatch):
    """STAC querying must retain parameters and time keys for the actual read."""
    query = Mock(
        return_value=Mock(
            status_code=200,
            json=Mock(return_value={"links": [{"title": "date", "variables": {"date": {"enum": ["19900102", "19900101"]}}}]}),
        )
    )
    monkeypatch.setattr(dates.requests, "get", query)
    request = {"param": [130, 131], "expver": "0001", "date": 19900101, "time": 0, "step": 0}
    original = deepcopy(request)
    catalog = source(request=request, bridge_start_date="stac", bridge_end_date="stac")
    catalog.to_dask()
    calls = opener.call_args_list
    assert [c.kwargs["config"] for c in calls] == ["/bridge.yaml", "/hpc.yaml"]
    assert calls[0].kwargs["enddate"] == "19900102T2300"
    assert calls[1].kwargs["startdate"] == "19900103T0000"
    assert all(c.args[0] == original for c in calls)
    assert request == original
    assert query.call_args.kwargs["params"] == {"param": 130, "expver": "0001", "root": "root"}
    assert query.call_args.kwargs["timeout"] == 10


@pytest.mark.parametrize(
    "failure, expected",
    [
        (requests.Timeout(), TimeoutError),
        (requests.ConnectionError(), ConnectionError),
    ],
)
def test_stac_errors_stop_retrieval(opener, monkeypatch, failure, expected):
    """Discovery failures cannot fall through to a stale storage selection."""
    monkeypatch.setattr(dates.requests, "get", Mock(side_effect=failure))
    with pytest.raises(expected):
        source(bridge_start_date="stac").to_dask()
    opener.assert_not_called()


def test_bridge_date_files(opener, monkeypatch):
    """Resolve legacy text-file boundaries before storage routing."""
    monkeypatch.setattr(timeutil.os.path, "isfile", lambda name: name in ("start.txt", "end.txt"))
    reader = mock_open()
    reader.side_effect = [
        mock_open(read_data="19900102T0000\n").return_value,
        mock_open(read_data="19900102T2300\n").return_value,
    ]
    monkeypatch.setattr(timeutil, "open", reader, raising=False)
    source(bridge_start_date="start.txt", bridge_end_date="end.txt").to_dask()
    assert [c.kwargs["config"] for c in opener.call_args_list] == ["/hpc.yaml", "/bridge.yaml", "/hpc.yaml"]
    assert opener.call_args_list[1].kwargs["startdate"] == "19900102T0000"
    assert opener.call_args_list[1].kwargs["enddate"] == "19900102T2300"


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"startdate": "19891231"}, "earlier than the data start"),
        ({"enddate": "19900104"}, "later than the data end"),
        ({"startdate": "19900103", "enddate": "19900102"}, "later than the end date"),
        ({"data_start_date": "19900104"}, "later than the end date"),
    ],
)
def test_outside_availability_rejected(opener, kwargs, match):
    """Fail before querying FDB when the requested window exceeds availability."""
    with pytest.raises(ValueError, match=match):
        source(**kwargs).to_dask()
    opener.assert_not_called()


@pytest.mark.parametrize(
    "freq, end, bridge_start, bridge_end, starts",
    [
        ("D", "19900103T2300", "19900102", "19900102T2300", ["19900101T0000", "19900102T0000", "19900103T0000"]),
        ("MS", "19900331", "19900201", "19900228T2300", ["19900101T0000", "19900201T0000", "19900301T0000"]),
        ("YS", "19921231", "19910101", "19911231T2300", ["19900101T0000", "19910101T0000", "19920101T0000"]),
    ],
)
def test_bridge_bounds_match_saved_cadence(opener, freq, end, bridge_start, bridge_end, starts):
    """End-of-period bridge dates must leave the following HPC field reachable."""
    source(savefreq=freq, data_end_date=end, bridge_start_date=bridge_start, bridge_end_date=bridge_end).to_dask()
    assert [c.kwargs["startdate"] for c in opener.call_args_list] == starts
    assert [c.kwargs["config"] for c in opener.call_args_list] == ["/hpc.yaml", "/bridge.yaml", "/hpc.yaml"]


def test_explicit_window_without_availability(opener):
    """Keep working when only the retrieval window was supplied."""
    source(data_start_date=None, data_end_date=None, startdate="19900101", enddate="19900102").to_dask()
    assert opener.call_args.kwargs["startdate"] == "19900101T0000"
    assert opener.call_args.kwargs["enddate"] == "19900102T0000"


def test_forecast_initial_offset_keeps_raw_origin(opener):
    """Date normalization must not change the origin used for the initial step."""
    source(data_start_date="19900101T0600", savefreq="D", timestyle="step").to_dask()
    assert opener.call_args.kwargs["data_start_date"] == "19900101T0600"


@pytest.mark.parametrize("filename", ["fdb_info_file.yaml", "fdb_info_hpc-only.yaml"])
def test_gsv_and_z3fdb_resolve_same_availability(monkeypatch, filename):
    """Both engines use the same existing workflow file and normalization rules."""
    monkeypatch.setattr(GSVSource, "_check_availability", lambda self: None)
    monkeypatch.setattr(GSVSource, "_post_init", lambda self: None)
    kwargs = dict(
        data_start_date="20000101",
        data_end_date="20000102",
        savefreq="D",
        metadata={"fdb_info_file": str(INFO_FILE.with_name(filename))},
    )
    gsv = GSVSource({"param": [130], "expver": "0001"}, engine="gsv", **kwargs)
    z3 = Z3FDB({"param": [130], "expver": "0001"}, engine="z3fdb", **kwargs)._resolve_dates()
    for attr in (
        "data_start_date",
        "data_end_date",
        "bridge_start_date",
        "bridge_end_date",
        "hpc_expver",
        "startdate",
        "enddate",
    ):
        assert getattr(z3, attr) == getattr(gsv, attr)
    assert z3.request == gsv._request


def test_missing_info_file_uses_catalog_dates(opener):
    """Retain GSV's fallback when a workflow info file is unavailable."""
    source(metadata={"fdb_info_file": "/unavailable-info.yaml", "fdb_path": "/hpc.yaml"}).to_dask()
    assert opener.call_args.kwargs["enddate"] == "19900103T2300"
    assert opener.call_args.args[0]["expver"] == "0001"

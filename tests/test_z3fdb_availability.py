"""Availability discovery exercised through Intake and external FDB requests."""

import os
from copy import deepcopy
from unittest.mock import Mock

import pandas as pd
import pytest
import requests

from aqua.core.intake_drivers.fdb import IntakeFDBSource
from aqua.core.intake_drivers.fdb.openers import dates
from aqua.core.intake_drivers.fdb.openers.gsv_source import GSVSource
from aqua.core.util import dump_yaml, load_yaml

pytestmark = pytest.mark.aqua
INFO_FILE = os.path.join(os.path.dirname(__file__), "catgen", "fdb_info_file.yaml")


@pytest.mark.parametrize("bridge_start", [None, "19900101T0000", "19900102T0000", "hpc-only"])
@pytest.mark.parametrize("engine", ["z3fdb", "gsv"])
def test_workflow_file_controls_routing(z3fdb_store, tmp_path, bridge_start, engine, request):
    """Workflow dates override stale catalog dates; missing bridge starts must retain bridge routing."""
    counts = [3] if bridge_start == "hpc-only" else [1, 1, 1] if bridge_start == "19900102T0000" else [2, 1]
    fdb = z3fdb_store([(count, 1, 12) for count in counts])
    info = load_yaml(INFO_FILE)
    if bridge_start == "hpc-only":
        info.pop("bridge")
    elif bridge_start is None:
        info["bridge"].pop("bridge_start_date")
    else:
        info["bridge"]["bridge_start_date"] = bridge_start
    info["data"]["expver"] = "a0h3"
    if "bridge" in info:
        info["bridge"]["expver"] = "0001"
    filename = os.path.join(tmp_path, "info.yaml")
    dump_yaml(filename, info)
    metadata = {**fdb.metadata, "fdb_info_file": filename}
    original = deepcopy(metadata)
    if engine == "gsv":
        request.getfixturevalue("gsv_boundary")
    result = IntakeFDBSource(
        {"param": [130], "expver": "old"},
        engine=engine,
        metadata=metadata,
        savefreq="D",
        chunks="S",
        data_start_date="20000101",
        data_end_date="20000102",
    ).to_dask()
    assert result.indexes["time"].equals(pd.date_range("19900101", periods=3, freq="D"))
    if engine == "z3fdb":
        stores = ["hpc"] if bridge_start == "hpc-only" else ["hpc", "bridge", "hpc"] if len(counts) == 3 else ["bridge", "hpc"]
        assert [call.config for call in fdb.calls] == [fdb.configs[name] for name in stores]
        assert [call.request["expver"] for call in fdb.calls] == ["a0h3" if name == "hpc" else "0001" for name in stores]
    assert metadata == original


def test_workflow_updates_are_seen_on_repeated_read(z3fdb_store, tmp_path):
    """Reading again must refresh the workflow file rather than reuse old dates or experiment IDs."""
    fdb = z3fdb_store([(2, 1, 12), (1, 1, 12), (3, 1, 12), (1, 1, 12)])
    info = load_yaml(INFO_FILE)
    filename = os.path.join(tmp_path, "info.yaml")
    dump_yaml(filename, info)
    source = IntakeFDBSource(
        {"param": [130]}, engine="z3fdb", savefreq="D", metadata={**fdb.metadata, "fdb_info_file": filename}
    )
    first = source.to_dask()
    info["data"].update(data_end_date="19900104T2300", expver="a0h4")
    info["bridge"].update(bridge_end_date="19900103T2300", expver="0002")
    dump_yaml(filename, info)
    second = source.to_dask()
    assert first.sizes["time"] == 3 and second.sizes["time"] == 4
    assert [call.request["expver"] for call in fdb.calls] == ["t039", "t039", "0002", "a0h4"]
    assert second.indexes["time"][-1] == pd.Timestamp("19900104")


@pytest.mark.parametrize("location", ["home", "path", "override-file", "override-home"])
def test_auto_dates_follow_effective_config(z3fdb_store, tmp_path, location):
    """Auto discovery must use the selected config and HPC expver, excluding the newest directory."""
    fdb = z3fdb_store((48, 1, 12))
    root = os.path.join(tmp_path, "archive")
    for day in (19900103, 19900101, 19900102):
        os.makedirs(os.path.join(root, f"d1:climate-dt:cmip6:hist:1:ifs-nemo:1:a0h3:clte:{day}"))
    os.makedirs(os.path.join(root, "d1:climate-dt:cmip6:hist:1:ifs-nemo:1:0001:clte:19910101"))
    config = fdb.configs["hpc"]
    home = os.path.dirname(os.path.dirname(os.path.dirname(config)))
    dump_yaml(config, {"type": "local", "spaces": [{"roots": [{"path": root}]}]})
    request = dict(
        param=[130],
        expver="0001",
        activity="CMIP6",
        experiment="hist",
        generation=1,
        model="IFS-NEMO",
        realization=1,
        stream="clte",
        dataset="climate-dt",
        **{"class": "d1"},
    )
    metadata = {"fdb_home": home} if location == "home" else {"fdb_path": config}
    override = config if location == "override-file" else home if location == "override-home" else None
    result = IntakeFDBSource(
        request,
        engine="z3fdb",
        metadata=metadata,
        config_fdb=override,
        hpc_expver="a0h3",
        data_start_date="auto",
        data_end_date="auto",
    ).to_dask()
    assert result.indexes["time"].equals(pd.date_range("19900101", periods=48, freq="h"))
    assert fdb.calls[0].request["date"] == "19900101/to/19900102"
    assert fdb.calls[0].request["expver"] == "a0h3"
    assert request["expver"] == "0001"


@pytest.mark.parametrize("failure, error", [(requests.Timeout(), TimeoutError), (requests.ConnectionError(), ConnectionError)])
def test_stac_failure_stops_retrieval(z3fdb_store, monkeypatch, failure, error):
    """A failed availability query must not fall through to stale FDB routing."""
    fdb = z3fdb_store((1, 1, 12))
    monkeypatch.setattr(dates.requests, "get", Mock(side_effect=failure))
    with pytest.raises(error, match="STAC API request"):
        IntakeFDBSource(
            {"param": [130]}, engine="z3fdb", bridge_start_date="stac", data_start_date="19900101", data_end_date="19900103"
        ).to_dask()
    assert not fdb.calls


@pytest.mark.parametrize("discovery", ["stac", "files"])
def test_bridge_discovery_retains_requests(z3fdb_store, tmp_path, monkeypatch, discovery):
    """Bridge discovery must select the right storage periods without stripping the field request."""
    counts = [48, 24] if discovery == "stac" else [24, 24, 24]
    fdb = z3fdb_store([(count, 1, 12) for count in counts])
    query = Mock(
        return_value=Mock(
            status_code=200,
            json=Mock(return_value={"links": [{"title": "date", "variables": {"date": {"enum": ["19900102", "19900101"]}}}]}),
        )
    )
    monkeypatch.setattr(dates.requests, "get", query)
    start, end = "stac", "stac"
    if discovery == "files":
        start, end = os.path.join(tmp_path, "start.txt"), os.path.join(tmp_path, "end.txt")
        for filename, value in [(start, "19900102T0000"), (end, "19900102T2300")]:
            with open(filename, "w") as stream:
                stream.write(value + "\n")
    request = {"param": [130], "expver": "0001", "date": 19900101, "time": 0, "step": 0}
    original = request.copy()
    result = IntakeFDBSource(
        request,
        engine="z3fdb",
        metadata=fdb.metadata,
        data_start_date="19900101",
        data_end_date="19900103T2300",
        bridge_start_date=start,
        bridge_end_date=end,
    ).to_dask()
    stores = ["bridge", "hpc"] if discovery == "stac" else ["hpc", "bridge", "hpc"]
    assert [call.config for call in fdb.calls] == [fdb.configs[name] for name in stores]
    assert result.indexes["time"].equals(pd.date_range("19900101", periods=72, freq="h"))
    assert all(call.request["param"] == [130] and call.request["expver"] == "0001" for call in fdb.calls)
    assert request == original
    if discovery == "stac":
        assert query.call_args.kwargs == {"params": {"param": 130, "expver": "0001", "root": "root"}, "timeout": 10}


@pytest.mark.parametrize(
    "kwargs, match",
    [
        ({"startdate": "19891231"}, "earlier than the data start"),
        ({"enddate": "19900104"}, "later than the data end"),
        ({"startdate": "19900103", "enddate": "19900102"}, "later than the end date"),
        ({"data_start_date": "19900104"}, "later than the end date"),
        ({"data_end_date": "auto", "timestyle": "yearmonth", "savefreq": "MS"}, "Auto date selection not supported"),
    ],
)
def test_invalid_availability_stops_retrieval(z3fdb_store, kwargs, match):
    """Invalid availability bounds must fail before any FDB retrieval."""
    fdb = z3fdb_store((1, 1, 12))
    options = dict(data_start_date="19900101", data_end_date="19900103T2300") | kwargs
    with pytest.raises(ValueError, match=match):
        IntakeFDBSource({"param": [130]}, engine="z3fdb", **options).to_dask()
    assert not fdb.calls


@pytest.mark.parametrize(
    "style, expected",
    [
        ("date", {"date": "20200101/to/20200301", "time": "0000/to/1200"}),
        ("yearmonth", {"year": "2020", "month": "01/to/03"}),
        ("step", {"date": "20200101", "time": "0000", "step": "0/to/1452"}),
    ],
)
def test_gsv_shared_formatting_unchanged(z3fdb_store, gsv_boundary, style, expected):
    """Shared time formatting must preserve GSV's schema and full-partition requests."""
    fdb = z3fdb_store((1, 1, 12))
    source = GSVSource(
        {"param": [130]},
        engine="gsv",
        metadata=fdb.metadata,
        timestyle=style,
        chunks="90D",
        timestep="h" if style == "date" else "1h",
        data_start_date="20200101",
        data_end_date="20200301T1200",
    )
    result = source.to_dask()
    assert result.indexes["time"].equals(pd.date_range("20200101", "20200301T1200", freq="h"))
    assert len(list(source.read_chunked())) == 1
    assert expected.items() <= gsv_boundary.call_args.args[0].items()


@pytest.mark.parametrize("explicit_bounds", [False, True])
def test_catalog_fallback_without_workflow_file(z3fdb_store, tmp_path, explicit_bounds):
    """An unavailable info file must retain catalog dates; explicit windows also work without availability bounds."""
    z3fdb_store((2, 1, 12))
    bounds = (
        dict(startdate="19900101", enddate="19900102")
        if explicit_bounds
        else dict(data_start_date="19900101", data_end_date="19900102")
    )
    result = IntakeFDBSource(
        {"param": [130]},
        engine="z3fdb",
        savefreq="D",
        metadata={"fdb_info_file": os.path.join(tmp_path, "missing.yaml")},
        **bounds,
    ).to_dask()
    assert result.indexes["time"].equals(pd.date_range("19900101", periods=2, freq="D"))


@pytest.mark.parametrize(
    "freq, end, start, stop, cadence",
    [
        ("D", "19900103T2300", "19900102", "19900102T2300", "D"),
        ("MS", "19900331", "19900201", "19900228T2300", "MS"),
        ("YS", "19921231", "19910101", "19911231T2300", "YS"),
    ],
)
def test_bridge_bounds_follow_saved_cadence(z3fdb_store, freq, end, start, stop, cadence):
    """An end-of-period bridge bound must leave the next saved field on HPC reachable."""
    fdb = z3fdb_store((1, 1, 12))
    result = IntakeFDBSource(
        {"param": [130]},
        engine="z3fdb",
        metadata=fdb.metadata,
        savefreq=freq,
        data_start_date="19900101",
        data_end_date=end,
        bridge_start_date=start,
        bridge_end_date=stop,
    ).to_dask()
    assert result.indexes["time"].equals(pd.date_range("19900101", periods=3, freq=cadence))
    assert [call.config for call in fdb.calls] == [fdb.configs[name] for name in ("hpc", "bridge", "hpc")]

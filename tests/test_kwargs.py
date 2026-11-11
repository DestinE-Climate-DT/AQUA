import pytest
from utils_tests import LOGLEVEL

from aqua import Reader
from aqua.core.backend.backend_intake_fdb import BackendIntakeFDB
from aqua.core.configurer import ConfigCatalog, ConfigContext
from aqua.core.util import dump_yaml


@pytest.mark.aqua
class TestKwargs:
    """Small class to test kwargs"""

    @pytest.mark.parametrize(
        "kwargs, variable, year",
        [
            pytest.param({}, "tos", 1985, id="default"),
            pytest.param({"year": 1986}, "tos", 1986, id="one-option"),
            pytest.param({"year": 1986, "variable": "a_ice", "fix": False}, "a_ice", 1986, id="two-options"),
            # With a wrong kwarg, the reader will remove it and log a warning
            pytest.param({"wrong_kwarg": "pippo"}, "tos", 1985, id="wrong-kwarg"),
        ],
    )
    def test_kwargs(self, kwargs, variable, year):
        """The source kwargs select the variable and the year; unknown ones are dropped."""
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d", loglevel=LOGLEVEL, **kwargs)
        data = reader.retrieve()
        assert list(data.data_vars) == [variable]
        assert data.time.dt.year == year


@pytest.mark.aqua
@pytest.mark.parametrize("entry_point", ["reader", "backend"])
@pytest.mark.parametrize(
    "catalog_engine,parameterized,kwargs,expected",
    [
        (None, False, {}, "gsv"),
        (None, False, {"engine": None}, "gsv"),
        (None, False, {"engine": "z3fdb"}, "z3fdb"),
        ("z3fdb", False, {}, "z3fdb"),
        ("z3fdb", False, {"engine": None}, "z3fdb"),
        ("z3fdb", False, {"engine": "gsv"}, "gsv"),
        ("z3fdb", True, {}, "z3fdb"),
        ("z3fdb", True, {"engine": "gsv"}, "gsv"),
        ("gsv", True, {"engine": "z3fdb"}, "z3fdb"),
        ("polytope", False, {}, "polytope"),
        ("z3fdb", False, {"engine": "polytope"}, "polytope"),
        ("polytope-gsv", False, {}, "polytope-gsv"),
    ],
)
def test_fdb_engine_precedence(tmp_path, monkeypatch, entry_point, catalog_engine, parameterized, kwargs, expected):
    """An implicit default must not override the catalog, or a catalog parameter an explicit engine."""
    context = ConfigContext()
    config = dict(context.config_dict)
    config["catalog"] = ["engine-test"]
    fixer, regrid = context.get_reader_folders()
    config["reader"] = {
        "catalog": str(tmp_path / "catalog.yaml"),
        "machine": str(tmp_path / "machine.yaml"),
        "fixer": fixer,
        "regrid": regrid,
    }
    dump_yaml(str(tmp_path / "config-aqua.yaml"), config)
    dump_yaml(str(tmp_path / "machine.yaml"), {"default": {}})
    for filename, name, child in [("catalog", "IFS", "model"), ("model", "test", "experiment")]:
        dump_yaml(
            str(tmp_path / f"{filename}.yaml"),
            {"sources": {name: {"driver": "yaml_file_cat", "args": {"path": str(tmp_path / f"{child}.yaml")}}}},
        )
    source = {"driver": "gsv", "args": {"request": {}}}
    if catalog_engine is not None:
        source["args"]["engine"] = catalog_engine
    if parameterized:
        source["parameters"] = {"engine": {"description": "Engine", "type": "str", "default": catalog_engine}}
        source["args"]["engine"] = "{{ engine }}"
    dump_yaml(str(tmp_path / "experiment.yaml"), {"metadata": {"machine": "lumi-44"}, "sources": {"fdb": source}})
    monkeypatch.setenv("AQUA_CONFIG", str(tmp_path))

    if entry_point == "reader":
        backend = Reader(model="IFS", exp="test", source="fdb", fix=False, datamodel=False, areas=False, **kwargs).backend
    else:
        backend = BackendIntakeFDB(model="IFS", exp="test", source="fdb", configurer_catalog=ConfigCatalog(), **kwargs)

    assert backend.engine == expected
    assert backend.kwargs["engine"] == expected
    assert (
        backend.esmcat.reader.func
        == {
            "gsv": "open_gsv",
            "z3fdb": "open_z3fdb",
            "polytope": "open_polytope",
            "polytope-gsv": "open_gsv",
        }[expected]
    )
    if expected == "polytope":
        assert backend.esmcat.reader.data.databridge == "lumi"

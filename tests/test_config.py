import json
import os
import shutil
import subprocess
import sys

import pytest

from aqua import show_catalog_content
from aqua.core.configurer import ConfigCatalog, ConfigContext
from aqua.core.util import dump_yaml


@pytest.mark.aqua
def test_config_plain():
    config = ConfigContext()
    assert "config-aqua.yaml" == os.path.basename(config.config_file)
    assert "ci" in config.config_dict["catalog"]


@pytest.mark.aqua
def test_config_paths():
    configfile = "tests/config/config-aqua-custom.yaml"

    configdir = ConfigContext().get_config_dir()

    # Copy the file to the config directory
    shutil.copy(configfile, configdir)

    config = ConfigCatalog(catalog="ci", configcontext=ConfigContext(filename="config-aqua-custom.yaml", configdir=configdir))
    paths, _ = config.get_machine_info()

    assert paths["paths"]["grids"] == "pluto"

    # Remove the copied file
    os.remove(f"{configdir}/config-aqua-custom.yaml")


@pytest.mark.aqua
def test_show_catalog_content_basic():
    """Test show_catalog_content with no filters."""
    results = show_catalog_content()

    assert isinstance(results, dict)
    # Check structure: catalog -> model -> exp -> list of sources
    for _, catalog_data in results.items():
        assert isinstance(catalog_data, dict)
        for _, model_data in catalog_data.items():
            assert isinstance(model_data, dict)
            for _, sources in model_data.items():
                assert isinstance(sources, list)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "location,overrides",
    [
        ("missing", None),
        ("env", None),
        ("env", {}),
        ("env", {"DEFAULT_NA_THRES": "auto"}),
        ("env", {"DEFAULT_ENGINE": None, "DEFAULT_NPROC": "8", "DEFAULT_NA_THRES": -1}),
        ("env", {"DEFAULT_NPROC": 8, "DEFAULT_NA_THRES": 0}),
        ("lower", {"DEFAULT_ENGINE": "z3fdb", "DEFAULT_NPROC": 8, "DEFAULT_NA_THRES": 0.25}),
        ("home", {"DEFAULT_DATAMODEL": False, "DEFAULT_NA_THRES": 1}),
        (
            "short",
            {
                "DEFAULT_DATAMODEL": "custom-datamodel",
                "DEFAULT_CONVENTION": "custom-convention",
                "DEFAULT_REGRID_METHOD": "bil",
                "DEFAULT_ENGINE": "z3fdb",
                "DEFAULT_NPROC": 2,
                "DEFAULT_DROP_GRID": "lon-lat-r025",
                "DEFAULT_DROP_CHUNK_TARGET": "256MiB",
                "DEFAULT_NA_THRES": 0.75,
            },
        ),
    ],
)
def test_config_defaults_at_import(tmp_path, location, overrides):
    """Configured defaults must reach consumer imports and captured argument defaults."""
    configdir = tmp_path / ".aqua" if location == "home" else tmp_path
    if location != "missing":
        if location == "lower":
            configured = {name.lower(): value for name, value in overrides.items()}
        elif location == "short":
            configured = {name.removeprefix("DEFAULT_").lower(): value for name, value in overrides.items()}
        else:
            configured = overrides
        dump_yaml(str(configdir / "config-aqua.yaml"), {} if configured is None else {"defaults": configured})
    expected = {
        "DEFAULT_DATAMODEL": "aqua",
        "DEFAULT_CONVENTION": "eccodes",
        "DEFAULT_REGRID_METHOD": "ycon",
        "DEFAULT_ENGINE": "gsv",
        "DEFAULT_NPROC": 4,
        "DEFAULT_DROP_GRID": "lon-lat-r100",
        "DEFAULT_DROP_CHUNK_TARGET": "128MiB",
        "DEFAULT_NA_THRES": "auto",
    }
    if location != "missing" and overrides:
        expected.update(overrides)
    script = """
import importlib
import json
import sys
from inspect import signature

from aqua import Drop, Fixer, Reader
from aqua.core import default
from aqua.core.drop.catalog_entry_builder import CatalogEntryBuilder

expected = json.loads(sys.argv[1])
assert {name: getattr(default, name) for name in expected} == expected
assert default.DEFAULT_REALIZATION == "r1"
consumers = {
    "backend.backend_factory": ("DEFAULT_DATAMODEL", "DEFAULT_CONVENTION"),
    "regridder.regridder": ("DEFAULT_REGRID_METHOD", "DEFAULT_NA_THRES"),
    "console.drop": ("DEFAULT_DROP_CHUNK_TARGET",),
}
for module, names in consumers.items():
    consumer = importlib.import_module("aqua.core." + module)
    assert {name: getattr(consumer, name) for name in names} == {name: expected[name] for name in names}
for function, argument, name in (
    (Reader, "nproc", "DEFAULT_NPROC"),
    (Reader, "engine", "DEFAULT_ENGINE"),
    (Fixer, "convention", "DEFAULT_CONVENTION"),
    (Drop, "chunk_target", "DEFAULT_DROP_CHUNK_TARGET"),
    (CatalogEntryBuilder.define_optimal_chunks, "chunk_target", "DEFAULT_DROP_CHUNK_TARGET"),
    (CatalogEntryBuilder.create_entry_details, "source_grid_name", "DEFAULT_DROP_GRID"),
):
    assert signature(function).parameters[argument].default == expected[name]
"""
    env = {**os.environ, "AQUA_CONFIG": str(tmp_path), "HOME": str(tmp_path)}
    result = subprocess.run(
        [sys.executable, "-c", script, json.dumps(expected)], env=env, capture_output=True, text=True, timeout=60
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.aqua
@pytest.mark.parametrize(
    "config,error",
    [
        ([], "config-aqua.yaml must contain a mapping"),
        ({"defaults": []}, "defaults must be a mapping"),
        ({"defaults": None}, "defaults must be a mapping"),
        ({"defaults": {"default_realization": "r2"}}, "unsupported default 'DEFAULT_REALIZATION'"),
        ({"defaults": {"realization": "r2"}}, "unsupported default 'DEFAULT_REALIZATION'"),
        ({"defaults": {1: "r2"}}, "unsupported default 1"),
    ],
)
def test_config_defaults_reject_invalid_config(tmp_path, config, error):
    """An invalid defaults section must fail at import instead of changing unrelated constants."""
    dump_yaml(str(tmp_path / "config-aqua.yaml"), config)
    env = {**os.environ, "AQUA_CONFIG": str(tmp_path), "HOME": str(tmp_path)}
    result = subprocess.run([sys.executable, "-c", "import aqua"], env=env, capture_output=True, text=True, timeout=60)
    assert result.returncode != 0
    last_line = result.stderr.strip().splitlines()[-1]
    assert last_line.startswith("ValueError: ")
    assert error in last_line

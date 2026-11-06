"""Testing for the catalog generator"""

import sys
from pathlib import Path

import pytest
from utils_tests import LOGLEVEL

from aqua.core.catgen import AquaFDBGenerator, get_nested
from aqua.core.console.main import AquaConsole
from aqua.core.util import dump_yaml, load_yaml

# Constants for vertical levels
ATMOS_PRESSURE_LEVELS = 19
NEMO_OCEAN_LEVELS = 75
FESOM_OCEAN_LEVELS = 69
ICON_OCEAN_LEVELS = 72


def _assert_source_levels(sources, source_key, expected_levels):
    """Helper to check vertical levels in a source."""
    assert len(sources["sources"][source_key]["metadata"]["levels"]) == expected_levels


def load_and_prepare(tmp_path, model, kind, reso, num_of_realizations=1, portfolio_path=None, **overrides):
    """
    Generate a catalog with `aqua catgen` run in-process and return the generated catalog sources.

    The catalog is written under tmp_path, so tests run in parallel do not share files.

    Args:
        tmp_path: Temporary directory provided by pytest (Path or str)
        model: Model to be checked (IFS-NEMO, IFS-FESOM, ICON)
        kind: Data portfolio type (minimal, reduced, full)
        reso: Resolution of the data (lowres, intermediate, production, etc.)
        num_of_realizations: Number of realizations for the ensemble
        portfolio_path: Data portfolio folder, the one of the test configuration if None
        **overrides: Other configuration keys to set (a value of None removes the key)

    Returns:
        dict: Catalog sources dictionary loaded from the generated YAML file
    """
    tmp_path = Path(tmp_path)
    config = _prepare_config(
        Path("tests/catgen/config-test-catgen.j2"),
        model,
        kind,
        reso,
        num_of_realizations,
        tmp_path,
        portfolio_path,
        **overrides,
    )
    _setup_catalog_directory(config)
    _run_catgen(config, kind, tmp_path)
    return _load_generated_sources(config)


def _prepare_config(template_path, model, kind, reso, num_of_realizations, tmp_path, portfolio_path=None, **overrides):
    """Prepare configuration from template with test-specific values."""
    definitions = {
        "model": model,
        "kind": kind,
        "resolution": reso,
        "num_of_realizations": num_of_realizations,
        "expid": "test",
    }
    config = load_yaml(str(template_path), definitions)

    # Use tmp_path for catalog directory to ensure test isolation
    config["repos"]["Climate-DT-catalog_path"] = str(tmp_path / "Climate-DT-catalog")
    if portfolio_path is not None:
        config["repos"]["data-portfolio_path"] = str(portfolio_path)
    for key, value in overrides.items():
        if value is None:
            config.pop(key, None)
        else:
            config[key] = value

    return config


def _catalog_dir(config):
    """Folder of the catalog of the configuration in the temporary Climate-DT-catalog."""
    return Path(config["repos"]["Climate-DT-catalog_path"]) / "catalogs" / config["catalog_dir"]


def _setup_catalog_directory(config):
    """Create the catalog.yaml that catgen requires, if the test did not write one."""
    catalog_yaml_path = _catalog_dir(config) / "catalog.yaml"
    catalog_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    if not catalog_yaml_path.exists():
        dump_yaml(str(catalog_yaml_path), {"sources": {}})


def _run_catgen(config, kind, tmp_path):
    """Run `aqua catgen` through the console, in the same process."""
    config_path = tmp_path / "test.yaml"
    dump_yaml(str(config_path), config)

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(sys, "argv", ["aqua", "catgen", "-p", kind, "-c", str(config_path), "-l", LOGLEVEL])
        AquaConsole().execute()


def _load_generated_sources(config):
    """Load the generated catalog entry, checking that main.yaml was written too."""
    entry_dir = _catalog_dir(config) / "catalog" / config["model"]
    assert (entry_dir / "main.yaml").exists(), f"main.yaml not found in {entry_dir}"
    return load_yaml(str(entry_dir / f"{config['exp']}.yaml"))


def _main_entry(**metadata):
    """Expected main.yaml entry of the experiment `linobanfi` of the test configuration (`metadata` overrides)."""
    return {
        "description": "FDB IFS/NEMO test run",
        "metadata": {
            "author": "test",
            "maintainer": "not specified",
            "machine": "test",
            "expid": "test",
            "resolution_atm": "tco2559",
            "resolution_oce": "eORCA1",
            "forcing": "highresmip-abrupt4xco2",
            "start": "1990",
            "dashboard": {"menu": "linobanfi", "resolution_id": "LR", "note": None},
            **metadata,
        },
        "driver": "yaml_file_cat",
        "args": {"path": "{{CATALOG_DIR}}/linobanfi.yaml"},
    }


def _assert_catalog_index(tmp_path, model, resolution_id):
    """Check main.yaml and catalog.yaml of the temporary catalog of the test configuration."""
    catalog = tmp_path / "Climate-DT-catalog" / "catalogs" / "ci"
    entry = _main_entry()
    entry["metadata"]["dashboard"]["resolution_id"] = resolution_id
    assert load_yaml(str(catalog / "catalog" / model / "main.yaml")) == {"sources": {"linobanfi": entry}}
    assert load_yaml(str(catalog / "catalog.yaml")) == {
        "sources": {
            model: {
                "description": f"{model} model",
                "driver": "yaml_file_cat",
                "args": {"path": f"{{{{CATALOG_DIR}}}}/catalog/{model}/main.yaml"},
            }
        }
    }


@pytest.fixture(scope="module")
def mini_portfolio(tmp_path_factory):
    """A tiny data portfolio (not the real one, which is a private repository) to run the generator offline."""
    root = tmp_path_factory.mktemp("data-portfolio")
    (root / "definitions").mkdir()
    (root / "portfolios" / "mini").mkdir(parents=True)
    dump_yaml(
        str(root / "definitions" / "levels.yaml"),
        {"plev2": {"levelist": [1, 2], "levels": [100, 200]}, "nemo3": {"levelist": [1, 2, 3], "levels": [0.5, 1.5, 2.5]}},
    )
    dump_yaml(
        str(root / "portfolios" / "mini" / "grids.yaml"),
        {
            "common": {"vertical-pl": "plev2", "vertical-NEMO-full": "nemo3"},
            "lowres": {
                "horizontal-IFS-NEMO-standard": "H32",
                "horizontal-IFS-NEMO-fine": "H128",
                "horizontal-ICON-standard": "H32",
            },
        },
    )
    dump_yaml(
        str(root / "portfolios" / "mini" / "portfolio.yaml"),
        {
            "ifs-nemo": [
                {"levtype": "sfc", "frequency": "monthly", "variables": [167, 168]},
                {"levtype": "sfc", "frequency": "monthly", "variables": [169], "omit-resolutions": ["fine"]},
                {"levtype": "pl", "frequency": "daily", "variables": [130], "vertical": "pl"},
                {
                    "levtype": "o3d",
                    "frequency": "monthly",
                    "variables": [263500],
                    "vertical": "NEMO-full",
                    "omit-resolutions": ["standard"],
                },
            ],
            "icon": [{"levtype": "sfc", "frequency": "monthly", "variables": [167]}],
        },
    )
    return root


@pytest.mark.catgen
@pytest.mark.parametrize("stream", ["clte", "clmn"])
@pytest.mark.parametrize("num_of_realizations", [1, 3])
def test_catgen_expid_metadata(tmp_path, stream, num_of_realizations):
    """Keep the Autosubmit ID in source metadata rather than the FDB request."""
    config = _prepare_config(
        Path("tests/catgen/config-test-catgen.j2"), "IFS-NEMO-5km", "full", "production", num_of_realizations, tmp_path
    )
    definitions = {
        **config,
        "source": "test-source",
        "model_base": "IFS-NEMO",
        "stream": stream,
        "param": 167,
        "variables": [167],
        "levtype": "sfc",
        "grid": "hpz10-nested",
        **AquaFDBGenerator.get_time("monthly" if stream == "clmn" else "hourly", "sfc"),
    }
    source = load_yaml("aqua/core/config/catgen/catalog_entry.j2", definitions=definitions, catgen=True)["test-source"]

    assert source["metadata"]["expid"] == config["expid"]
    assert "expid" not in source["args"]["request"]
    assert source["args"]["request"]["expver"] == config["expver"]


@pytest.mark.parametrize(
    ("model,kind,reso,nsources,nocelevels,ensemble"),
    [
        ("IFS-NEMO", "minimal", "lowres", 4, NEMO_OCEAN_LEVELS, 5),
        ("IFS-NEMO", "reduced", "intermediate", 9, NEMO_OCEAN_LEVELS, 5),
        ("IFS-NEMO-5km", "full", "production", 28, NEMO_OCEAN_LEVELS, 1),
        ("IFS-FESOM", "full", "production", 31, FESOM_OCEAN_LEVELS, 1),
        ("ICON-10km", "full", "production", 27, ICON_OCEAN_LEVELS, 1),
    ],
)
@pytest.mark.catgen
def test_catgen_portfolios(tmp_path, model, kind, reso, nsources, nocelevels, ensemble):
    """Test catalog generation for different portfolios and models."""
    sources = load_and_prepare(tmp_path=tmp_path, model=model, kind=kind, reso=reso, num_of_realizations=ensemble)
    _assert_catalog_index(tmp_path, model, {"lowres": "LR", "intermediate": "MR", "production": "HR"}[reso])

    # Check number of sources
    assert len(sources["sources"]) == nsources

    for source in sources["sources"].values():
        assert source["metadata"]["expid"] == "test"
        assert "expid" not in source["args"]["request"]

    # Check vertical levels based on portfolio and model
    if kind == "minimal":
        _assert_source_levels(sources, "monthly-hpz5-pl", ATMOS_PRESSURE_LEVELS)
        _assert_source_levels(sources, "monthly-hpz5-o3d", nocelevels)
        # Check ensemble production for minimal
        assert sources["sources"]["monthly-hpz5-pl"]["parameters"]["realization"]["allowed"] == [*range(1, ensemble + 1)]

    elif kind == "reduced":
        grid = "hpz7"
        _assert_source_levels(sources, f"monthly-{grid}-pl", ATMOS_PRESSURE_LEVELS)
        _assert_source_levels(sources, f"monthly-{grid}-o3d", nocelevels)
        # Check ensemble production for reduced
        assert sources["sources"][f"monthly-{grid}-pl"]["parameters"]["realization"]["allowed"] == [*range(1, ensemble + 1)]

    elif kind == "full":
        _assert_source_levels(sources, "hourly-hpz10-pl", ATMOS_PRESSURE_LEVELS)
        _assert_source_levels(sources, "daily-hpz10-o3d", nocelevels)


MANDATORY_KEYS_TO_TEST = [
    "author",
    "model",
    "repos.data-portfolio_path",
]


@pytest.mark.parametrize("missing_key", MANDATORY_KEYS_TO_TEST)
@pytest.mark.catgen
def test_catgen_missing_key(tmp_path, missing_key):
    """
    Ensure that a ValueError is raised when a required configuration key is missing.
    """
    config_path = "tests/catgen/config-test-catgen.j2"
    config = load_yaml(config_path)

    if missing_key.startswith("repos."):
        subkey = missing_key.split(".", 1)[1]
        if "repos" in config and subkey in config["repos"]:
            del config["repos"][subkey]
    else:
        config.pop(missing_key, None)

    dump_path = tmp_path / "test.yaml"
    dump_yaml(str(dump_path), config)

    with pytest.raises(ValueError, match="Missing required configuration keys"):
        AquaFDBGenerator(config_path=dump_path, data_portfolio="minimal")


# Test cases for get_nested function
@pytest.mark.catgen
def test_get_nested():
    """Test get_nested function with various dictionary structures."""
    # Flat dictionary
    assert get_nested({"a": 1}, "a") == 1
    assert get_nested({}, "z") is None

    # Nested dictionary with tuple path
    data = {"repos": {"path": "/data"}}
    assert get_nested(data, ("repos", "path")) == "/data"
    assert get_nested({"repos": {}}, ("repos", "path")) is None


# ── static methods ───────────────────────────────────────────────────────────
@pytest.mark.catgen
@pytest.mark.parametrize(
    "freq,levtype,expected_chunks",
    [
        ("hourly", "pl", "6h"),
        ("hourly", "sfc", "D"),
        ("6-hourly", "pl", "6h"),
        ("daily", "pl", "D"),
        ("monthly", "pl", "MS"),
    ],
)
def test_get_time(freq, levtype, expected_chunks):
    """Test get_time static method for different frequencies."""
    assert AquaFDBGenerator.get_time(freq, levtype)["chunks"] == expected_chunks


@pytest.mark.catgen
def test_get_time_unknown_raises():
    """Verify that unknown frequency raises KeyError."""
    with pytest.raises(KeyError):
        AquaFDBGenerator.get_time("yearly", "pl")


@pytest.mark.catgen
def test_get_levelist():
    """Test get_levelist static method with and without vertical profiles."""
    # Without vertical
    lev, vals = AquaFDBGenerator.get_levelist({"levtype": "sfc"}, {}, {})
    assert lev is None and vals is None

    # With vertical profile
    profile = {"vertical": "pl-19"}
    grids = {"vertical-pl-19": "lev_pl19"}
    levels = {"lev_pl19": {"levelist": [1, 2], "levels": [100, 200]}}
    lev, vals = AquaFDBGenerator.get_levelist(profile, grids, levels)
    assert lev == [1, 2]


@pytest.mark.catgen
def test_get_value_from_map():
    """Test get_value_from_map static method."""
    mapping = {"production": "HR"}
    assert AquaFDBGenerator.get_value_from_map("production", mapping, "resolution") == "HR"

    with pytest.raises(ValueError, match="Unexpected resolution"):
        AquaFDBGenerator.get_value_from_map("unknown", mapping, "resolution")


# generate_catalog — edge cases
@pytest.mark.catgen
def test_generate_catalog_no_resolutions(tmp_path, mini_portfolio):
    """If local_grids is empty, generate_catalog returns early and writes no catalog (it would fail if it wrote one)."""
    config = _prepare_config(
        Path("tests/catgen/config-test-catgen.j2"), "IFS-NEMO", "mini", "lowres", 1, tmp_path, mini_portfolio
    )
    dump_yaml(str(tmp_path / "test.yaml"), config)

    gen = AquaFDBGenerator(data_portfolio="mini", config_path=str(tmp_path / "test.yaml"))
    gen.local_grids = {}
    gen.generate_catalog()

    assert not (tmp_path / "Climate-DT-catalog").exists()


@pytest.mark.catgen
def test_catgen_mini_portfolio(tmp_path, mini_portfolio):
    """Check every part of a generated catalog on a portfolio small enough to read: sources, grids, levels, index files.

    It would fail if `omit-resolutions` were ignored, if the variables of two profiles feeding one source were not merged,
    or if the levels, the grid strings or the main.yaml entry were wrong.
    """
    sources = load_and_prepare(tmp_path, "IFS-NEMO", "mini", "lowres", 2, mini_portfolio)["sources"]

    assert sorted(sources) == [
        "daily-hpz5-pl",
        "daily-hpz7-pl",
        "monthly-hpz5-sfc",
        "monthly-hpz7-o3d",
        "monthly-hpz7-sfc",
    ]
    # the second sfc profile is omitted at the fine (hpz7) resolution, and adds its variable at the standard one (hpz5)
    assert sources["monthly-hpz5-sfc"]["metadata"]["variables"] == [167, 168, 169]
    assert sources["monthly-hpz7-sfc"]["metadata"]["variables"] == [167, 168]
    assert sources["monthly-hpz7-sfc"]["metadata"]["source_grid_name"] == "hpz7-nested"
    assert sources["daily-hpz5-pl"]["metadata"]["levels"] == [100, 200]
    assert sources["daily-hpz5-pl"]["args"]["request"]["levelist"] == [1, 2]
    assert sources["monthly-hpz7-o3d"]["metadata"]["levels"] == [0.5, 1.5, 2.5]
    assert sources["monthly-hpz7-o3d"]["metadata"]["source_grid_name"] == "nemo-eORCA1-hpz7-nested-3d-v3"
    assert sources["monthly-hpz5-sfc"]["args"]["request"]["stream"] == "clmn"
    assert sources["daily-hpz5-pl"]["args"]["request"]["stream"] == "clte"
    assert sources["daily-hpz5-pl"]["parameters"]["realization"]["allowed"] == [1, 2]

    _assert_catalog_index(tmp_path, "IFS-NEMO", "LR")


@pytest.mark.catgen
def test_catgen_keeps_existing_catalog_entries(tmp_path, mini_portfolio):
    """A second experiment is added to main.yaml and a second model to catalog.yaml, the existing entries are kept.

    It would fail if main.yaml or catalog.yaml were rewritten from scratch, losing what other runs had put in them.
    """
    first = _main_entry()
    other_model = {"description": "OTHER model", "driver": "yaml_file_cat", "args": {"path": "other/main.yaml"}}
    catalog = tmp_path / "Climate-DT-catalog" / "catalogs" / "ci"
    (catalog / "catalog" / "IFS-NEMO").mkdir(parents=True)
    dump_yaml(str(catalog / "catalog.yaml"), {"sources": {"OTHER": other_model}})
    dump_yaml(str(catalog / "catalog" / "IFS-NEMO" / "main.yaml"), {"sources": {"old-exp": first}})

    load_and_prepare(tmp_path, "IFS-NEMO", "mini", "lowres", 1, mini_portfolio)

    assert set(load_yaml(str(catalog / "catalog.yaml"))["sources"]) == {"OTHER", "IFS-NEMO"}
    assert load_yaml(str(catalog / "catalog.yaml"))["sources"]["OTHER"] == other_model
    assert load_yaml(str(catalog / "catalog" / "IFS-NEMO" / "main.yaml"))["sources"] == {
        "old-exp": first,
        "linobanfi": _main_entry(),
    }


@pytest.mark.catgen
def test_catgen_no_ocean_grid_for_model(tmp_path, mini_portfolio):
    """ICON has no ocean grid at low resolution: the generator raises instead of writing a catalog without it."""
    with pytest.raises(ValueError, match="No ocean grid available for: icon lowres"):
        load_and_prepare(tmp_path, "ICON", "mini", "lowres", 1, mini_portfolio, ocean_grid=None, atm_grid=None)

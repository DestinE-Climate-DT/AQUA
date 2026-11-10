"""
Tests for the aqua.core.analysis module.

Organized into focused test classes covering:
- Configuration setters (set* methods)
- Integration flows (set* → run* with mocked subprocess)
- Utilities (helpers, path resolution)
- Templating (jinja rendering)
- CLI parser
- Edge cases
"""

import argparse
import os
import sys
from types import SimpleNamespace
from unittest.mock import patch

import pytest
from jinja2 import UndefinedError
from utils_tests import LOGLEVEL

import aqua.core
from aqua.core.analysis import Analysis
from aqua.core.configurer import ConfigContext
from aqua.core.console.analysis import analysis_parser
from aqua.core.util import dump_yaml, load_yaml

pytestmark = pytest.mark.console


# ======================================================================
# Analysis fixtures
# ======================================================================
@pytest.fixture(scope="function")
def analysis():
    """Fresh Analysis instance per test (function scope to avoid state leakage)."""
    return Analysis(loglevel=LOGLEVEL)


@pytest.fixture(scope="session")
def kinds_yaml(tmp_path_factory):
    """Experiment kinds reference with historical and scenario."""
    kinds_dir = tmp_path_factory.mktemp("kinds")
    kinds_file = kinds_dir / "kinds.yaml"
    data = {
        "historical": {"period": "past", "forcing": "CMIP6"},
        "scenario": {"period": "future", "forcing": "SSP5-8.5"},
        "default": {"period": "unknown", "forcing": "unknown"},
    }
    dump_yaml(str(kinds_file), data)
    return str(kinds_file)


@pytest.fixture(scope="function")
def temp_env(tmp_path):
    """Temporary directory structure with dummy scripts and config files (function scope)."""

    # Create directories
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()

    config_dir = tmp_path / "config"
    config_dir.mkdir()

    # Create dummy scripts
    script_biases = scripts_dir / "biases.py"
    script_biases.touch()
    script_ecmean = scripts_dir / "ecmean.py"
    script_ecmean.touch()

    # Create minimal config files
    cfg_biases = config_dir / "biases.yaml"
    dump_yaml(str(cfg_biases), {"key": "biases_value"})
    cfg_ecmean = config_dir / "ecmean.yaml"
    dump_yaml(str(cfg_ecmean), {"key": "ecmean_value"})

    # Create jinja template config
    cfg_jinja = config_dir / "jinja_template.yaml"
    cfg_jinja.write_text("model: '{{ period }}'\nsource: '{{ forcing }}'\n")

    outdir = tmp_path / "output"

    return {
        "scripts": str(scripts_dir),
        "script_biases": str(script_biases),
        "script_ecmean": str(script_ecmean),
        "config": str(config_dir),
        "cfg_biases": str(cfg_biases),
        "cfg_ecmean": str(cfg_ecmean),
        "cfg_jinja": str(cfg_jinja),
        "outdir": str(outdir),
        "cli": {
            "biases": str(script_biases),
            "ecmean": str(script_ecmean),
        },
    }


@pytest.fixture(scope="function")
def configured_analysis(analysis, temp_env):
    """Analysis with the experiment and output directory set, as the setters leave it."""
    analysis.model = "IFS"
    analysis.exp = "test-tco79"
    analysis.source = "short"
    analysis.catalog = "test_catalog"
    analysis.realization = "r1"
    analysis.output_dir = temp_env["outdir"]
    return analysis


def _run_collection(analysis, diag_config, cli):
    """Run a diagnostic collection with the tool subprocess replaced by a mock, and return the mock."""
    with patch.object(analysis, "run_diagnostic_tool") as mock_tool:
        analysis.run_diagnostic_collection(collection="atm", diag_config=diag_config, cli=cli)
    return mock_tool


# ============================================================================
# TestConfigurationSetters: Verify each set* method correctly sets state/guards
# ============================================================================
class TestConfigurationSetters:
    """Tests for configuration setter methods."""

    def test_set_catalog_model_exp_source_happy_path(self, analysis):
        """set_catalog_model_exp_source correctly sets model, exp, source, and auto-resolves catalog."""
        args = argparse.Namespace(model="IFS", exp="test-tco79", source="short", catalog=None, source_oce=None)
        config = {}
        analysis.set_catalog_model_exp_source(args, config)

        assert analysis.model == "IFS"
        assert analysis.exp == "test-tco79"
        assert analysis.source == "short"
        assert analysis.source_oce is None
        assert analysis.catalog == "ci"  # the catalog that contains the triplet

    @pytest.mark.parametrize("missing_field", ["model", "exp", "source"])
    def test_set_catalog_model_exp_source_missing_required(self, analysis, missing_field):
        """Missing model, exp, or source triggers sys.exit."""
        args_dict = {"model": "IFS", "exp": "test-tco79", "source": "short", "catalog": None, "source_oce": None}
        args_dict[missing_field] = None
        args = argparse.Namespace(**args_dict)
        config = {}

        with pytest.raises(SystemExit):
            analysis.set_catalog_model_exp_source(args, config)

    def test_set_catalog_model_exp_source_explicit_catalog(self, analysis):
        """Explicit catalog bypasses auto-detection: the triplet is in `ci`, the requested catalog is kept."""
        args = argparse.Namespace(model="IFS", exp="test-tco79", source="short", catalog="my_catalog", source_oce=None)
        config = {}

        analysis.set_catalog_model_exp_source(args, config)

        assert analysis.catalog == "my_catalog"

    def test_set_startdate_enddate_provided(self, analysis):
        """Dates are correctly set when provided."""
        args = argparse.Namespace(startdate="2020-01-01", enddate="2020-12-31")
        config = {}

        analysis.set_startdate_enddate(args, config)
        assert analysis.startdate == "2020-01-01"
        assert analysis.enddate == "2020-12-31"

    def test_set_startdate_enddate_none(self, analysis):
        """Dates remain None when not provided."""
        args = argparse.Namespace(startdate=None, enddate=None)
        config = {}

        analysis.set_startdate_enddate(args, config)
        assert analysis.startdate is None
        assert analysis.enddate is None

    @pytest.mark.parametrize("regrid, expected", [("r100", "r100"), ("False", None), ("false", None), (False, None)], ids=str)
    def test_set_regrid_option(self, analysis, regrid, expected):
        """A grid name enables regridding; False, 'False' or 'false' disable it (None)."""
        analysis.set_regrid_option(argparse.Namespace(regrid=regrid), {})
        assert analysis.regrid == expected

    @pytest.mark.parametrize(
        "args, expected",
        [
            pytest.param({"realization": "r2"}, "r2", id="name-kept"),
            pytest.param({"realization": "2"}, "r2", id="digit-string-prefixed"),
            pytest.param({"realization": 3}, "r3", id="int-prefixed"),
            pytest.param({"realization": None}, "r1", id="none-default"),
            pytest.param({"target": "ensemble", "realization": None}, "ensemble", id="ensemble-target"),
            pytest.param({"target": "ensemble", "realization": "r5"}, "ensemble", id="ensemble-target-wins"),
            pytest.param({"target": "single", "realization": "r3"}, "r3", id="other-target"),
        ],
    )
    def test_set_realization(self, analysis, args, expected):
        """Realizations get the 'r' prefix and default to r1; the ensemble target replaces any realization."""
        analysis.set_realization(argparse.Namespace(**args), {})
        assert analysis.realization == expected

    def test_set_output_directory_custom_path(self, analysis, tmp_path):
        """Output directory is constructed correctly with custom outputdir."""
        args = argparse.Namespace(
            model="IFS",
            exp="test-tco79",
            source="short",
            catalog="my_catalog",
            source_oce=None,
            outputdir=f"{tmp_path}/output_dir",
        )
        config = {}
        analysis.set_catalog_model_exp_source(args, config)  # Set catalog/model/exp for output path
        analysis.set_realization(args, config)  # Ensure realization is set for output path
        analysis.set_output_directory(args, config)
        assert analysis.output_dir == f"{tmp_path}/output_dir/my_catalog/IFS/test-tco79/r1"

    def test_set_output_directory_default_path(self, analysis, tmp_path, monkeypatch):
        """Output directory defaults to ./output when not provided."""
        monkeypatch.chdir(tmp_path)  # the default output directory is created in the current directory
        args = argparse.Namespace(
            model="IFS", exp="test-tco79", source="short", catalog="my_catalog", source_oce=None, outputdir=None
        )
        config = {}
        analysis.set_catalog_model_exp_source(args, config)  # Set catalog/model/exp for output path
        analysis.set_realization(args, config)  # Ensure realization is set for output path
        analysis.set_output_directory(args, config)
        assert analysis.output_dir == "./output/my_catalog/IFS/test-tco79/r1"

    def test_set_serial_or_parallel_serial(self, analysis):
        """Serial mode is set correctly when --serial is True."""
        args = argparse.Namespace(serial=True, nworkers=None, nthreads=3)
        analysis.set_serial_or_parallel(args)
        assert analysis.serial is True


# ============================================================================
# TestIntegrationFlow: Full set* → run* with mocked subprocess
# ============================================================================
class TestIntegrationFlow:
    """Integration tests: setters → diagnostic collection with mocked subprocess."""

    def test_run_diagnostic_collection_happy_path(self, analysis, temp_env):
        """Happy path: set* methods → run_diagnostic_collection with mocked run_diagnostic_tool."""
        # Configure analysis
        args = argparse.Namespace(realization="r1")
        job_config = {
            "model": "IFS",
            "exp": "test-tco79",
            "source": "short",
            "catalog": "test_catalog",
            "outputdir": temp_env["outdir"],
        }
        analysis.set_catalog_model_exp_source(args, job_config)
        analysis.set_realization(args, job_config)
        analysis.set_output_directory(args, job_config)

        config = {"biases": {"config": temp_env["cfg_biases"]}}

        with patch.object(analysis, "run_diagnostic_tool") as mock_tool:
            analysis.run_diagnostic_collection(
                collection="atm",
                diag_config=config,
                cli=temp_env["cli"],
            )

        mock_tool.assert_called_once()
        call_kwargs = mock_tool.call_args.kwargs
        assert "--model IFS" in call_kwargs["extra_args"]
        assert "--exp test-tco79" in call_kwargs["extra_args"]
        assert "--source short" in call_kwargs["extra_args"]

    @pytest.mark.parametrize(
        "setup, tool_config, expected, absent",
        [
            pytest.param(
                {"serial": True},
                {"nworkers": 4, "nthreads": 2},
                [],
                ["--nworkers", "--nthreads"],
                id="serial-no-workers",
            ),
            pytest.param(
                {"serial": False},
                {"nworkers": 4, "nthreads": 2},
                ["--nworkers 4", "--nthreads 2"],
                [],
                id="parallel-workers",
            ),
            pytest.param({"regrid": None}, {}, [], ["--regrid"], id="no-regrid"),
            pytest.param({"regrid": "r100"}, {}, ["--regrid r100"], [], id="regrid"),
            pytest.param({"source_oce": "short-oce"}, {"source_oce": True}, ["--source_oce short-oce"], [], id="source-oce"),
            pytest.param({"cluster": "tcp://127.0.0.1:8786"}, {}, ["--cluster tcp://127.0.0.1:8786"], [], id="cluster"),
            pytest.param({"cluster": "tcp://127.0.0.1:8786"}, {"nocluster": True}, [], ["--cluster"], id="nocluster"),
        ],
    )
    def test_run_diagnostic_collection_extra_args(self, configured_analysis, temp_env, setup, tool_config, expected, absent):
        """Options of the analysis and of the tool config become CLI arguments of the tool, and only when they apply."""
        analysis = configured_analysis
        if "serial" in setup:
            analysis.set_serial_or_parallel(argparse.Namespace(serial=setup["serial"], nworkers=4, nthreads=2))
        if "regrid" in setup:
            analysis.regrid = setup["regrid"]
        if "source_oce" in setup:
            analysis.source_oce = setup["source_oce"]
        if "cluster" in setup:
            analysis.dask_cluster = SimpleNamespace(address=setup["cluster"])  # a running cluster, without starting one

        mock_tool = _run_collection(analysis, {"biases": {"config": temp_env["cfg_biases"], **tool_config}}, temp_env["cli"])

        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert all(arg in extra_args for arg in expected)
        assert not any(arg in extra_args for arg in absent)

    def test_run_diagnostic_collection_multiple_configs(self, configured_analysis, temp_env):
        """Multiple configs produce multiple tool calls with indexed logfiles."""
        cfg_file2 = os.path.join(temp_env["config"], "config2.yaml")
        dump_yaml(cfg_file2, {"key": "value2"})

        mock_tool = _run_collection(
            configured_analysis, {"biases": {"config": [temp_env["cfg_biases"], cfg_file2]}}, temp_env["cli"]
        )

        assert mock_tool.call_count == 2
        logfiles = [c.kwargs["logfile"] for c in mock_tool.call_args_list]
        assert logfiles[0].endswith("-1.log")
        assert logfiles[1].endswith("-2.log")

    @pytest.mark.parametrize(
        "diag_config, cli",
        [
            pytest.param({}, "valid", id="empty-config"),
            pytest.param("biases", {}, id="tool-missing-from-cli"),
            pytest.param("biases", {"biases": "/nonexistent/script.py"}, id="script-missing"),
        ],
    )
    def test_run_diagnostic_collection_skips(self, configured_analysis, temp_env, diag_config, cli):
        """No tool runs without a config, without a CLI entry for the tool or when its script does not exist."""
        diag_config = {"biases": {"config": temp_env["cfg_biases"]}} if diag_config == "biases" else diag_config
        cli = temp_env["cli"] if cli == "valid" else cli

        mock_tool = _run_collection(configured_analysis, diag_config, cli)

        mock_tool.assert_not_called()


# ============================================================================
# TestUtilities: Helpers and utility methods
# ============================================================================
class TestUtilities:
    """Tests for utility methods."""

    def test_build_extra_args_all_none(self, analysis):
        """All None values return empty string."""
        result = analysis.build_extra_args(catalog=None, model=None, startdate=None)
        assert result == ""

    def test_build_extra_args_single_kwarg(self, analysis):
        """Single non-None kwarg produces correct flag."""
        result = analysis.build_extra_args(model="IFS")
        assert "--model IFS" in result

    @pytest.mark.parametrize(
        "kwargs,expected",
        [
            ({"model": "IFS", "exp": "test", "source": "lra"}, ["--model IFS", "--exp test", "--source lra"]),
            ({"catalog": "test_catalog", "realization": "r1"}, ["--catalog test_catalog", "--realization r1"]),
            ({"startdate": "2020-01-01", "enddate": "2020-12-31"}, ["--startdate 2020-01-01", "--enddate 2020-12-31"]),
        ],
    )
    def test_build_extra_args_multiple(self, analysis, kwargs, expected):
        """Multiple kwargs produce all expected flags."""
        result = analysis.build_extra_args(**kwargs)
        for flag in expected:
            assert flag in result

    def test_build_extra_args_skips_none_values(self, analysis):
        """None values are skipped, non-None included."""
        result = analysis.build_extra_args(model="IFS", exp=None, source="lra")
        assert "--model IFS" in result
        assert "--exp" not in result
        assert "--source lra" in result

    def test_get_aqua_paths(self, analysis):
        """get_aqua_paths resolves core and diagnostics paths."""
        core_path, diag_path, config_dir = analysis.get_aqua_paths()
        assert core_path == os.path.dirname(aqua.core.__file__)
        assert config_dir == ConfigContext().configdir


# ============================================================================
# TestTemplating: Jinja rendering and experiment kind configuration
# ============================================================================
class TestTemplating:
    """Tests for templating and experiment kind configuration."""

    def test_configure_experiment_kind_none(self, analysis, kinds_yaml):
        """exp_kind=None defaults to default"""
        analysis.configure_experiment_kind(None, kinds_yaml)
        assert analysis.exp_kind_dict["period"] == "unknown"

    def test_configure_experiment_kind_file_not_found(self, analysis):
        """Non-existent file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            analysis.configure_experiment_kind("historical", "/nonexistent/file.yaml")

    def test_configure_experiment_kind_valid(self, analysis, kinds_yaml):
        """Valid kind key returns its sub-dictionary."""
        analysis.configure_experiment_kind("historical", kinds_yaml)
        assert analysis.exp_kind_dict["period"] == "past"
        assert analysis.exp_kind_dict["forcing"] == "CMIP6"

    def test_configure_experiment_kind_unknown_no_default_raises(self, analysis, tmp_path):
        """Unknown kind with no 'default' key raises KeyError."""
        # Create kinds file WITHOUT default key
        kinds_file = tmp_path / "kinds_no_default.yaml"
        dump_yaml(
            str(kinds_file),
            {
                "historical": {"period": "past", "forcing": "CMIP6"},
                "scenario": {"period": "future", "forcing": "SSP5-8.5"},
            },
        )

        with pytest.raises(KeyError):
            analysis.configure_experiment_kind("unknown_kind", str(kinds_file))

    def test_configure_experiment_kind_unknown_with_default_key(self, analysis, kinds_yaml, aqua_caplog):
        """An unknown kind falls back on the 'default' entry of the file, with a warning."""
        caplog = aqua_caplog(analysis.logger)
        analysis.configure_experiment_kind("nonexistent", kinds_yaml)
        assert analysis.exp_kind_dict == load_yaml(kinds_yaml)["default"]
        assert analysis.exp_kind_dict["period"] == "unknown"
        warnings = [record.getMessage() for record in caplog.records if record.levelname == "WARNING"]
        expected = f"Experiment kind 'nonexistent' not found in config file '{kinds_yaml}': using the 'default' entry"
        assert warnings == [expected]

    def test_configure_experiment_kind_multiple_kinds(self, analysis, kinds_yaml):
        """Different kinds return independent sub-dictionaries."""
        analysis.configure_experiment_kind("historical", kinds_yaml)
        assert analysis.exp_kind_dict["forcing"] == "CMIP6"

        analysis.configure_experiment_kind("scenario", kinds_yaml)
        assert analysis.exp_kind_dict["forcing"] == "SSP5-8.5"

    def test_configure_template_configs_jinja_substitution(self, analysis, temp_env, kinds_yaml):
        """Jinja placeholders are replaced by exp_kind_dict values."""
        analysis.exp_kind_dict = load_yaml(kinds_yaml)["historical"]
        result_paths = analysis.configure_template_configs([temp_env["cfg_jinja"]])

        assert len(result_paths) == 1
        rendered = load_yaml(result_paths[0])
        assert rendered["model"] == "past"
        assert rendered["source"] == "CMIP6"

    def test_configure_template_configs_multiple(self, analysis, temp_env, kinds_yaml):
        """All configs are rendered and returned."""
        analysis.exp_kind_dict = load_yaml(kinds_yaml)["historical"]

        # Create second template with different variables
        cfg2 = os.path.join(temp_env["config"], "jinja2.yaml")
        with open(cfg2, "w") as f:
            f.write("tag: '{{ period }}'\n")

        result_paths = analysis.configure_template_configs([temp_env["cfg_jinja"], cfg2])
        assert len(result_paths) == 2

        # Check first config
        rendered1 = load_yaml(result_paths[0])
        assert rendered1["model"] == "past"
        assert rendered1["source"] == "CMIP6"

        # Check second config
        rendered2 = load_yaml(result_paths[1])
        assert rendered2["tag"] == "past"

    def test_configure_template_configs_undefined_var_raises(self, analysis, temp_env):
        """Undefined jinja var raises (strict mode)."""
        analysis.exp_kind_dict = {"unrelated": "x"}

        with pytest.raises(UndefinedError):
            analysis.configure_template_configs([temp_env["cfg_jinja"]])

    def test_configure_template_configs_shared_temp_dir(self, analysis, temp_env, kinds_yaml):
        """All rendered configs land in one shared temporary directory."""
        analysis.exp_kind_dict = load_yaml(kinds_yaml)["historical"]

        cfg2 = os.path.join(temp_env["config"], "jinja2.yaml")
        with open(cfg2, "w") as f:
            f.write("key: '{{ forcing }}'\n")

        result_paths = analysis.configure_template_configs([temp_env["cfg_jinja"], cfg2])
        dirs = {os.path.dirname(p) for p in result_paths}
        assert len(dirs) == 1


# ============================================================================
# TestParser: CLI argument parser
# ============================================================================
class TestParser:
    """Tests for analysis_parser."""

    def test_parser_defaults(self):
        """All default values match documented argparse configuration."""
        parser = analysis_parser()
        args = parser.parse_args([])

        assert args.regrid == "False"
        assert args.loglevel == "INFO"
        assert args.serial is False
        assert args.model is None
        assert args.exp is None
        assert args.source is None

    @pytest.mark.parametrize(
        "flag_combo,expected",
        [
            (
                ["--model", "IFS", "--exp", "test-tco79", "--source", "short"],
                {"model": "IFS", "exp": "test-tco79", "source": "short"},
            ),
            (
                ["--regrid", "r100", "--serial", "--loglevel", LOGLEVEL],
                {"regrid": "r100", "serial": True, "loglevel": "DEBUG"},
            ),
            (
                ["-m", "ERA5", "-e", "monthly", "-s", "data"],
                {"model": "ERA5", "exp": "monthly", "source": "data"},
            ),
        ],
    )
    def test_parser_flags(self, flag_combo, expected):
        """All parser flags are parsed correctly."""
        parser = analysis_parser()
        args = parser.parse_args(flag_combo)

        for key, value in expected.items():
            assert getattr(args, key) == value


# ============================================================================
# TestEdgeCases: Edge cases and error conditions
# ============================================================================
class TestEdgeCases:
    """Edge cases and boundary conditions."""

    @pytest.mark.parametrize("exit_code", [0, 3])
    def test_run_command_logs_and_returns_exit_code(self, analysis, temp_env, exit_code):
        """run_command creates the log directory, writes the output of the command there and returns its exit code."""
        log_file = os.path.join(temp_env["outdir"], "subdir", "test.log")
        cmd = f"{sys.executable} -c \"import sys; print('hello'); sys.exit({exit_code})\""

        assert analysis.run_command(cmd, log_file) == exit_code
        with open(log_file) as f:
            assert f.read() == "hello\n"

    def test_run_setup_checker_integration(self, configured_analysis):
        """run_setup_checker builds and runs command with correct args."""
        analysis = configured_analysis
        analysis.aqua_diagnostics_path = "/fake/aqua/diagnostics"

        with patch.object(analysis, "run_command", return_value=0) as mock_cmd:
            result = analysis.run_setup_checker()

        assert result == 0
        mock_cmd.assert_called_once()
        cmd_arg = mock_cmd.call_args[0][0]
        assert "--model IFS" in cmd_arg
        assert "--exp test-tco79" in cmd_arg

    def test_exp_kind_dict_renders_and_cleans_up(self, configured_analysis, temp_env, kinds_yaml):
        """exp_kind_dict renders the configs passed to the tool and removes them after running it."""
        analysis = configured_analysis
        analysis.exp_kind_dict = load_yaml(kinds_yaml)["historical"]
        rendered = {}

        def read_rendered_config(**kwargs):
            """Read the rendered config while it exists, as the tool would."""
            path = kwargs["extra_args"].split("--config ")[-1]
            rendered[path] = load_yaml(path)

        with patch.object(analysis, "run_diagnostic_tool", side_effect=read_rendered_config):
            analysis.run_diagnostic_collection(
                collection="atm", diag_config={"biases": {"config": temp_env["cfg_jinja"]}}, cli=temp_env["cli"]
            )

        ((path, content),) = rendered.items()
        assert content == {"model": "past", "source": "CMIP6"}
        assert not os.path.exists(os.path.dirname(path))

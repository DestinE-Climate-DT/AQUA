"""
Test some utilities and functions in the aqua.analysis module.

The more structured test of aqua analysis console command is
in tests/test_console.py
"""

import argparse
import os

import pytest

from aqua.core.analysis import run_command, run_diagnostic_collection
from aqua.core.analysis.analysis import _build_extra_args, configure_experiment_kind, configure_template_configs
from aqua.core.console.analysis import analysis_parser
from aqua.core.logger import log_configure
from aqua.core.util import load_yaml

logger = log_configure("DEBUG", "test_analysis")

pytestmark = pytest.mark.aqua


def test_run_command():
    """Test the run_command function."""
    command = "echo 'Hello, World!'"

    with pytest.raises(TypeError):
        # Test with missing log_file argument
        _ = run_command(command, logger=logger)


def test_run_diagnostic_collection():
    """Test the run_diagnostic_collection function."""

    res = run_diagnostic_collection(collection="pluto", diag_config={}, logger=logger)
    assert res is None, "Expected None return value for empty config"

    config = {
        "diagnostics": {
            "pluto": {
                "nworkers": 1,
                "config": "pippo.yaml",
            }
        }
    }

    # Go through run_diagnostic_collection and fail
    # at the final run_diagnostic call.
    # The fail is a return code != 0 so there is no
    # raise Exception, we just check that the function
    # completes without errors.
    run_diagnostic_collection(
        collection="pluto",
        parallel=True,
        regrid="r100",
        logger=logger,
        diag_config=config,
        cluster=True,
        catalog="test_catalog",
        realization="r2",
    )

    assert True, "run_diagnostic_collection should complete without errors"


def test_build_extra_args_with_dates():
    """Test that _build_extra_args correctly formats startdate and enddate."""

    result = _build_extra_args(catalog="test_catalog", realization="r1", startdate="2020-01-01", enddate="2020-12-31")

    assert "--catalog test_catalog" in result
    assert "--realization r1" in result
    assert "--startdate 2020-01-01" in result
    assert "--enddate 2020-12-31" in result


def test_build_extra_args_without_dates():
    """Test that _build_extra_args skips None values."""

    result = _build_extra_args(catalog="test_catalog", startdate=None, enddate=None)

    assert "--catalog test_catalog" in result
    assert "--startdate" not in result
    assert "--enddate" not in result


# ── Layer 1: Parser ───────────────────────────────────────────────────────────


class TestAnalysisParser:
    """Tests for analysis_parser() — no mocking required."""

    def test_defaults(self):
        """All default values match the documented argparse configuration."""
        parser = analysis_parser()
        args = parser.parse_args([])

        assert args.threads == -1
        assert args.regrid == "False"
        assert args.loglevel == "INFO"
        assert args.parallel is False
        assert args.local_clusters is False
        assert args.model is None
        assert args.exp is None
        assert args.source is None
        assert args.catalog is None
        assert args.outputdir is None
        assert args.config is None
        assert args.kind is None
        assert args.source_oce is None
        assert args.realization is None
        assert args.startdate is None
        assert args.enddate is None

    def test_all_long_flags(self):
        """All long-form flags are parsed to the correct namespace attributes."""
        parser = analysis_parser()
        args = parser.parse_args(
            [
                "--catalog",
                "my_catalog",
                "--model",
                "IFS",
                "--exp",
                "test-tco79",
                "--source",
                "lra-r100-monthly",
                "--source_oce",
                "lra-r100-monthly-oce",
                "--realization",
                "r1",
                "--startdate",
                "2020-01-01",
                "--enddate",
                "2020-12-31",
                "--regrid",
                "r200",
                "--outputdir",
                "/tmp/output",
                "--config",
                "/tmp/config.yaml",
                "--kind",
                "historical",
                "--parallel",
                "--local_clusters",
                "--threads",
                "4",
                "--loglevel",
                "DEBUG",
            ]
        )

        assert args.catalog == "my_catalog"
        assert args.model == "IFS"
        assert args.exp == "test-tco79"
        assert args.source == "lra-r100-monthly"
        assert args.source_oce == "lra-r100-monthly-oce"
        assert args.realization == "r1"
        assert args.startdate == "2020-01-01"
        assert args.enddate == "2020-12-31"
        assert args.regrid == "r200"
        assert args.outputdir == "/tmp/output"
        assert args.config == "/tmp/config.yaml"
        assert args.kind == "historical"
        assert args.parallel is True
        assert args.local_clusters is True
        assert args.threads == 4
        assert args.loglevel == "DEBUG"

    def test_short_flags(self):
        """Short-form flags map to the same attributes as their long-form equivalents."""
        parser = analysis_parser()
        args = parser.parse_args(
            [
                "-c",
                "my_catalog",
                "-m",
                "IFS",
                "-e",
                "test-tco79",
                "-s",
                "lra-r100-monthly",
                "-d",
                "/tmp/output",
                "-f",
                "/tmp/config.yaml",
                "-k",
                "historical",
                "-p",
                "-t",
                "8",
                "-l",
                "WARNING",
            ]
        )

        assert args.catalog == "my_catalog"
        assert args.model == "IFS"
        assert args.exp == "test-tco79"
        assert args.source == "lra-r100-monthly"
        assert args.outputdir == "/tmp/output"
        assert args.config == "/tmp/config.yaml"
        assert args.kind == "historical"
        assert args.parallel is True
        assert args.threads == 8
        assert args.loglevel == "WARNING"

    def test_loglevel_uppercased(self):
        """Lowercase loglevel strings are coerced to uppercase by the parser."""
        parser = analysis_parser()
        args = parser.parse_args(["-l", "debug"])
        assert args.loglevel == "DEBUG"

    def test_loglevel_invalid_raises_system_exit(self):
        """An unrecognised loglevel value causes the parser to call sys.exit."""
        parser = analysis_parser()
        with pytest.raises(SystemExit) as exc_info:
            parser.parse_args(["--loglevel", "VERBOSE"])
        assert exc_info.value.code != 0

    def test_existing_parser_extended(self):
        """Passing an existing ArgumentParser extends it without conflicts."""
        base = argparse.ArgumentParser()
        base.add_argument("--extra", type=str, default="yes")
        extended = analysis_parser(base)
        args = extended.parse_args(["--extra", "no", "--model", "IFS"])

        assert args.extra == "no"
        assert args.model == "IFS"


# ── Layer 2: Pure Functions ───────────────────────────────────────────────────


class TestBuildExtraArgsExtended:
    """Extended coverage for _build_extra_args."""

    def test_all_none_returns_empty_string(self):
        """When every value is None the output is an empty string."""
        result = _build_extra_args(catalog=None, model=None, startdate=None)
        assert result == ""

    def test_single_kwarg(self):
        """A single non-None kwarg produces the correct flag string."""
        result = _build_extra_args(model="IFS")
        assert result.strip() == "--model IFS"

    def test_flag_order_follows_kwargs(self):
        """Flags appear in the same order as kwargs (Python 3.7+ dict ordering)."""
        result = _build_extra_args(model="IFS", exp="test", source="lra")
        assert result.index("--model") < result.index("--exp") < result.index("--source")

    def test_none_values_skipped(self):
        """None values are silently omitted while non-None values are included."""
        result = _build_extra_args(model="IFS", exp=None, source="lra")
        assert "--model IFS" in result
        assert "--exp" not in result
        assert "--source lra" in result


class TestConfigureExperimentKind:
    """Tests for configure_experiment_kind — uses tmp_path, no mocking."""

    def test_none_exp_kind_returns_none(self):
        """exp_kind=None returns None without accessing any file."""
        result = configure_experiment_kind(None, "/nonexistent/file.yaml", logger)
        assert result is None

    def test_missing_file_raises_file_not_found(self, tmp_path):
        """A non-existent exp_kind_file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError):
            configure_experiment_kind("historical", str(tmp_path / "missing.yaml"), logger)

    def test_valid_kind_returned(self, tmp_path):
        """A matching kind key returns its sub-dictionary."""
        kind_file = tmp_path / "kinds.yaml"
        kind_file.write_text("historical:\n  period: past\nscenario:\n  period: future\n")

        result = configure_experiment_kind("historical", str(kind_file), logger)

        assert result["period"] == "past"

    def test_unknown_kind_returns_string_default(self, tmp_path):
        """A kind absent from the YAML returns the string literal 'default'."""
        kind_file = tmp_path / "kinds.yaml"
        kind_file.write_text("historical:\n  period: past\n")

        result = configure_experiment_kind("unknown_kind", str(kind_file), logger)

        assert result == "default"

    def test_multiple_kinds_isolated(self, tmp_path):
        """Different kind keys each return their own independent sub-dictionary."""
        kind_file = tmp_path / "kinds.yaml"
        kind_file.write_text("historical:\n  period: past\n  forcing: CMIP6\nscenario:\n  period: future\n  forcing: SSP5\n")

        hist = configure_experiment_kind("historical", str(kind_file), logger)
        scen = configure_experiment_kind("scenario", str(kind_file), logger)

        assert hist["forcing"] == "CMIP6"
        assert scen["forcing"] == "SSP5"


class TestConfigureTemplateConfigs:
    """Tests for configure_template_configs — uses tmp_path, no mocking."""

    def test_jinja_variables_substituted(self, tmp_path):
        """Jinja2 placeholders in config files are replaced by exp_kind_dict values."""
        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("model: {{ name }}\nsource: {{ src }}\n")
        definitions = {"name": "IFS", "src": "lra-r100"}

        result_paths = configure_template_configs([str(cfg_file)], definitions, logger)

        assert len(result_paths) == 1
        rendered = load_yaml(result_paths[0])
        assert rendered["model"] == "IFS"
        assert rendered["source"] == "lra-r100"

    def test_multiple_configs_all_rendered(self, tmp_path):
        """All configs in the list are rendered and returned."""
        cfg1 = tmp_path / "a.yaml"
        cfg2 = tmp_path / "b.yaml"
        cfg1.write_text("tag: {{ val }}\n")
        cfg2.write_text("tag: {{ val }}\n")
        definitions = {"val": "hello"}

        result_paths = configure_template_configs([str(cfg1), str(cfg2)], definitions, logger)

        assert len(result_paths) == 2
        for path in result_paths:
            rendered = load_yaml(path)
            assert rendered["tag"] == "hello"

    def test_all_outputs_in_same_temp_dir(self, tmp_path):
        """All rendered configs land in a single shared temporary directory."""
        cfg1 = tmp_path / "a.yaml"
        cfg2 = tmp_path / "b.yaml"
        cfg1.write_text("key: value1\n")
        cfg2.write_text("key: value2\n")

        result_paths = configure_template_configs([str(cfg1), str(cfg2)], {"x": "y"}, logger)

        dirs = {os.path.dirname(p) for p in result_paths}
        assert len(dirs) == 1, "All rendered configs should be in the same temp dir"

    def test_output_filenames_match_input_basenames(self, tmp_path):
        """The rendered file's basename equals the original config's basename."""
        cfg_file = tmp_path / "my_config.yaml"
        cfg_file.write_text("key: value\n")

        result_paths = configure_template_configs([str(cfg_file)], {}, logger)

        assert os.path.basename(result_paths[0]) == "my_config.yaml"

    def test_temp_dir_uses_expected_prefix(self, tmp_path):
        """The temporary directory name starts with the expected AQUA prefix."""
        cfg_file = tmp_path / "cfg.yaml"
        cfg_file.write_text("key: value\n")

        result_paths = configure_template_configs([str(cfg_file)], {}, logger)

        temp_dir_name = os.path.basename(os.path.dirname(result_paths[0]))
        assert temp_dir_name.startswith("aqua_analysis_configs_")

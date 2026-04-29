"""
Test some utilities and functions in the aqua.analysis module.

The more structured test of aqua analysis console command is
in tests/test_console.py
"""

import os
from unittest.mock import patch

import pytest

from aqua.core.analysis import run_command, run_diagnostic_collection
from aqua.core.analysis.analysis import (
    _build_extra_args,
    configure_experiment_kind,
    configure_template_configs,
)
from aqua.core.console.analysis import analysis_parser
from aqua.core.logger import log_configure
from aqua.core.util import dump_yaml, load_yaml

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


# ── Layer 2b: run_diagnostic_collection (mocked subprocess boundary) ─────────
@pytest.fixture(scope="module")
def tool_env(tmp_path_factory):
    """Minimal filesystem for run_diagnostic_collection tests: a dummy script and config."""
    base = tmp_path_factory.mktemp("tool_env")
    script = base / "biases.py"
    script.touch()
    cfg = base / "config.yaml"
    dump_yaml(str(cfg), {"key": "value"})
    outdir = base / "output"
    return {
        "script": str(script),
        "cfg": str(cfg),
        "outdir": str(outdir),
        "cli": {"biases": str(script)},
    }


# this calss patches the run_diagnostic_tool function,
# which is the last step in run_diagnostic_collection before subprocess calls.
# By patching here, we can test all the logic in run_diagnostic_collection without actually invoking any subprocesses.
# Each test can then assert how run_diagnostic_tool was called (or not called) based on different inputs and configurations.
PATCH_TOOL = "aqua.core.analysis.analysis.run_diagnostic_tool"


class TestRunDiagnosticCollection:
    """Tests for run_diagnostic_collection — run_diagnostic_tool is always mocked."""

    def _base_config(self, tool_env):
        return {"biases": {"config": tool_env["cfg"]}}

    def test_empty_diag_config_no_calls(self, tool_env):
        """An empty diag_config triggers no tool invocations."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config={},
                cli=tool_env["cli"],
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        mock_tool.assert_not_called()

    def test_missing_cli_path_skips_tool(self, tool_env):
        """A tool absent from cli dict is skipped silently."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli={},
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        mock_tool.assert_not_called()

    def test_missing_script_file_skips_tool(self, tool_env):
        """A cli path pointing to a non-existent file is skipped."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli={"biases": "/nonexistent/script.py"},
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        mock_tool.assert_not_called()

    def test_happy_path_calls_run_diagnostic_tool(self, tool_env):
        """Happy path: run_diagnostic_tool called once with correct core args."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli=tool_env["cli"],
                model="IFS",
                exp="test-tco79",
                source="lra-r100",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        mock_tool.assert_called_once()
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--model IFS" in extra_args
        assert "--exp test-tco79" in extra_args
        assert "--source lra-r100" in extra_args
        assert f"--config {tool_env['cfg']}" in extra_args

    def test_regrid_flag_added(self, tool_env):
        """regrid='r100' appends --regrid r100 to extra_args."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli=tool_env["cli"],
                regrid="r100",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--regrid r100" in extra_args

    def test_parallel_adds_nworkers(self, tool_env):
        """parallel=True with nworkers in config appends --nworkers."""
        config = {"biases": {"config": tool_env["cfg"], "nworkers": 4}}
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=config,
                cli=tool_env["cli"],
                parallel=True,
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--nworkers 4" in extra_args

    def test_cluster_flag_added(self, tool_env):
        """cluster address is forwarded as --cluster when nocluster is not set."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli=tool_env["cli"],
                cluster="tcp://scheduler:8786",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--cluster tcp://scheduler:8786" in extra_args

    def test_nocluster_suppresses_cluster_flag(self, tool_env):
        """nocluster=True in tool config prevents --cluster from being added."""
        config = {"biases": {"config": tool_env["cfg"], "nocluster": True}}
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=config,
                cli=tool_env["cli"],
                cluster="tcp://scheduler:8786",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--cluster" not in extra_args

    def test_source_oce_added_when_allowed(self, tool_env):
        """source_oce is forwarded when source_oce=True is set in tool config."""
        config = {"biases": {"config": tool_env["cfg"], "source_oce": True}}
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=config,
                cli=tool_env["cli"],
                source_oce="lra-r100-oce",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--source_oce lra-r100-oce" in extra_args

    def test_source_oce_skipped_when_not_allowed(self, tool_env):
        """source_oce is NOT forwarded when source_oce is absent from tool config."""
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli=tool_env["cli"],
                source_oce="lra-r100-oce",
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        extra_args = mock_tool.call_args.kwargs["extra_args"]
        assert "--source_oce" not in extra_args

    def test_multiple_configs_indexed_logfiles(self, tool_env, tmp_path):
        """Two configs produce two calls; logfiles are suffixed with -1 and -2."""
        cfg2 = tmp_path / "config2.yaml"
        dump_yaml(str(cfg2), {"key": "value2"})
        config = {"biases": {"config": [tool_env["cfg"], str(cfg2)]}}
        with patch(PATCH_TOOL) as mock_tool:
            run_diagnostic_collection(
                collection="atm",
                diag_config=config,
                cli=tool_env["cli"],
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        assert mock_tool.call_count == 2
        logfiles = [c.kwargs["logfile"] for c in mock_tool.call_args_list]
        assert logfiles[0].endswith("-1.log")
        assert logfiles[1].endswith("-2.log")

    def test_exp_kind_dict_renders_and_cleans_up(self, tool_env):
        """exp_kind_dict triggers template rendering; temp dir is removed afterwards."""
        rendered_cfg = tool_env["cfg"]
        with (
            patch(PATCH_TOOL),
            patch(
                "aqua.core.analysis.analysis.configure_template_configs",
                return_value=[rendered_cfg],
            ) as mock_render,
            patch("aqua.core.analysis.analysis.shutil.rmtree") as mock_rm,
        ):
            run_diagnostic_collection(
                collection="atm",
                diag_config=self._base_config(tool_env),
                cli=tool_env["cli"],
                exp_kind_dict={"period": "past"},
                output_dir=tool_env["outdir"],
                logger=logger,
            )
        mock_render.assert_called_once()
        mock_rm.assert_called_once()


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

    def test_build_extra_args_with_dates(self):
        """Test that _build_extra_args correctly formats startdate and enddate."""
        result = _build_extra_args(catalog="test_catalog", realization="r1", startdate="2020-01-01", enddate="2020-12-31")
        assert "--catalog test_catalog" in result
        assert "--realization r1" in result
        assert "--startdate 2020-01-01" in result
        assert "--enddate 2020-12-31" in result

    def test_build_extra_args_without_dates(self):
        """Test that _build_extra_args skips None values."""
        result = _build_extra_args(catalog="test_catalog", startdate=None, enddate=None)
        assert "--catalog test_catalog" in result
        assert "--startdate" not in result
        assert "--enddate" not in result


@pytest.fixture(scope="module")
def kinds_yaml(tmp_path_factory):
    """Shared kinds.yaml written once for the whole module."""
    kind_file = tmp_path_factory.mktemp("kinds") / "kinds.yaml"
    data = {
        "historical": {"period": "past", "forcing": "CMIP6"},
        "scenario": {"period": "future", "forcing": "SSP5"},
    }
    dump_yaml(str(kind_file), data)
    return kind_file


@pytest.fixture(scope="module")
def jinja_cfg_yaml(tmp_path_factory):
    """Shared Jinja template config written once for the whole module."""
    cfg_file = tmp_path_factory.mktemp("jinja") / "config.yaml"
    dump_yaml(str(cfg_file), {"model": "{{ period }}", "source": "{{ forcing }}"})
    return cfg_file


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

    def test_valid_kind_returned(self, kinds_yaml):
        """A matching kind key returns its sub-dictionary."""
        result = configure_experiment_kind("historical", str(kinds_yaml), logger)
        assert result["period"] == "past"

    def test_unknown_kind_returns_string_default(self, kinds_yaml):
        """A kind absent from the YAML returns the string literal 'default'."""
        result = configure_experiment_kind("unknown_kind", str(kinds_yaml), logger)
        assert result == "default"

    def test_multiple_kinds_isolated(self, kinds_yaml):
        """Different kind keys each return their own independent sub-dictionary."""
        hist = configure_experiment_kind("historical", str(kinds_yaml), logger)
        scen = configure_experiment_kind("scenario", str(kinds_yaml), logger)

        assert hist["forcing"] == "CMIP6"
        assert scen["forcing"] == "SSP5"


class TestConfigureTemplateConfigs:
    """Tests for configure_template_configs — uses tmp_path, no mocking."""

    def test_jinja_variables_substituted(self, jinja_cfg_yaml, kinds_yaml):
        """Jinja2 placeholders in config files are replaced by exp_kind_dict values."""
        definitions = load_yaml(str(kinds_yaml))["historical"]

        result_paths = configure_template_configs([str(jinja_cfg_yaml)], definitions, logger)

        assert len(result_paths) == 1
        rendered = load_yaml(result_paths[0])
        assert rendered["model"] == "past"
        assert rendered["source"] == "CMIP6"

    def test_multiple_configs_all_rendered(self, tmp_path):
        """All configs in the list are rendered and returned."""
        cfg1 = tmp_path / "a.yaml"
        cfg2 = tmp_path / "b.yaml"
        dump_yaml(str(cfg1), {"tag": "{{ val }}"})
        dump_yaml(str(cfg2), {"tag": "{{ val }}"})
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
        dump_yaml(str(cfg1), {"key": "value1"})
        dump_yaml(str(cfg2), {"key": "value2"})

        result_paths = configure_template_configs([str(cfg1), str(cfg2)], {"x": "y"}, logger)

        dirs = {os.path.dirname(p) for p in result_paths}
        assert len(dirs) == 1, "All rendered configs should be in the same temp dir"

    def test_output_filenames_match_input_basenames(self, tmp_path):
        """The rendered file's basename equals the original config's basename."""
        cfg_file = tmp_path / "my_config.yaml"
        dump_yaml(str(cfg_file), {"key": "value"})

        result_paths = configure_template_configs([str(cfg_file)], {}, logger)

        assert os.path.basename(result_paths[0]) == "my_config.yaml"

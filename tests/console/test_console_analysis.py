"""Console analysis workflow tests."""

import logging
import os
import sys
from unittest.mock import patch

import pytest
from utils_tests import LOGLEVEL

import aqua
from aqua.core.analysis import Analysis
from aqua.core.console import push_analysis
from aqua.core.util import dump_yaml

pytestmark = pytest.mark.console


@pytest.mark.xdist_group(name="console_analysis_operations")
class TestAquaConsoleAnalysis:
    """Analysis operations against an isolated AQUA installation."""

    @pytest.mark.parametrize("exit_code", [0, 1])
    def test_console_analysis_setup_checker(self, aqua_install, run_aqua, tmp_path, exit_code):
        """A setup checker that exits with 1 stops the analysis with exit code 1, one that succeeds does not.

        The checker is a real script run by the console, with no mock (the test used to patch `run_setup_checker` to
        raise SystemExit(1) and assert the SystemExit it had raised itself). Its log is written in both cases."""
        checker = tmp_path / "cli_setup_checker.py"
        checker.write_text(f"import sys\nsys.exit({exit_code})\n")
        analysis_cfg = tmp_path / "config.aqua-analysis-test.yaml"
        dump_yaml(
            str(analysis_cfg),
            {
                "job": {
                    "run_setup_checker": True,
                    "outputdir": str(tmp_path / "output"),
                    "catalog": "ci",
                    "model": "IFS",
                    "exp": "test-tco79",
                    "source": "lra-r100-monthly",
                },
                "cli": {"setup_checker": str(checker)},
                "run": [[]],  # nothing to run after the checker
            },
        )
        command = ["analysis", "--config", str(analysis_cfg), "--setup_checker", "--serial"]

        if exit_code == 0:
            run_aqua(command)
        else:
            with pytest.raises(SystemExit) as exc_info:
                run_aqua(command)
            assert exc_info.value.code == 1
        assert (tmp_path / "output" / "ci" / "IFS" / "test-tco79" / "r1" / "setup_checker.log").is_file()

    def test_console_analysis_minimal(self, aqua_install, run_aqua, tmp_path, catalog_copy_path):
        """Minimal smoke test: verifies aqua analysis routes through the config without
        running real subprocesses."""
        run_aqua(["catalog", "add", "ci", "-e", catalog_copy_path])

        dummy_script = tmp_path / "cli_dummy.py"
        dummy_script.touch()

        diag_cfg = tmp_path / "diag_config.yaml"
        dump_yaml(str(diag_cfg), {"key": "value"})

        analysis_cfg = tmp_path / "config.aqua-analysis-test.yaml"
        dump_yaml(
            str(analysis_cfg),
            {
                "job": {
                    "run_setup_checker": False,
                    "loglevel": "WARNING",
                    "outputdir": str(tmp_path / "output"),
                },
                "cli": {"dummy_tool": str(dummy_script)},
                "run": [["dummy"]],
                "diagnostics": {
                    "dummy": {
                        "dummy_tool": {"config": str(diag_cfg)},
                    }
                },
            },
        )

        with patch("aqua.core.analysis.Analysis.run_diagnostic_tool") as mock_tool:
            run_aqua(
                [
                    "analysis",
                    "--config",
                    str(analysis_cfg),
                    "--catalog",
                    "ci",
                    "-m",
                    "ERA5",
                    "-e",
                    "era5-hpz3",
                    "-s",
                    "monthly",
                    "-l",
                    LOGLEVEL,
                ]
            )

        output_dir = tmp_path / "output" / "ci" / "ERA5" / "era5-hpz3" / "r1"
        assert output_dir.exists(), f"Output directory not created: {output_dir}"

        mock_tool.assert_called_once()


# ======================================================================
# Project-aware analysis and push
# ======================================================================
CONFIG_FILES = ("config.aqua-analysis.yaml", "config.aqua-analysis-ensemble.yaml", "config.grouping.yaml")


@pytest.fixture
def analysis_project(project_install, tmp_path, monkeypatch):
    """Return a function building an installation (see `project_install`) whose analysis folders hold analysis configs.

    Each config runs one tool with `--config <label>|${AQUA_PROJECT}|${AQUA_PROJECT_PATH}`, where the label is the
    config path relative to the `analysis` folder: the recorded command tells which config was used and what was
    exported. A custom config sits in tmp_path. The projects are `alpha` and `beta` (names of no real project, whose
    installed package would win), with packages `aqua.alpha` and `aqua.beta` under tmp_path.
    """
    monkeypatch.delenv("AQUA_PROJECT_PATH", raising=False)
    site = tmp_path / "site" / "aqua"
    for project in ("alpha", "beta"):
        (site / project).mkdir(parents=True)
        (site / project / "__init__.py").touch()
    monkeypatch.setattr(aqua, "__path__", [*aqua.__path__, str(site)])
    script = tmp_path / "cli_tool.py"
    script.touch()

    def _write_configs(folder):
        for name in CONFIG_FILES:
            path = os.path.join(folder, name)
            label = name if folder == str(tmp_path) else os.path.relpath(path, os.path.join(tmp_path, "analysis"))
            job = {"catalog": "ci", "model": "IFS", "exp": "test", "source": "lra", "outputdir": str(tmp_path / "output")}
            dump_yaml(
                path,
                {
                    "job": {**job, "report": False},
                    "cli": {"tool": str(script)},
                    "run": [["collection"]],
                    "diagnostics": {"collection": {"tool": {"config": f"{label}|${{AQUA_PROJECT}}|${{AQUA_PROJECT_PATH}}"}}},
                },
            )

    def _build(project="alpha", folders=("analysis/alpha", "analysis/beta")):
        project_install(project, folders=folders)
        _write_configs(str(tmp_path))
        for folder in folders:
            _write_configs(os.path.join(tmp_path, folder))
        return site

    yield _build
    for project in ("alpha", "beta"):
        sys.modules.pop(f"aqua.{project}", None)


@pytest.fixture
def recorded_commands(monkeypatch):
    """Record the diagnostic tool commands instead of running them."""
    commands = []

    def _record(self, cmd, log_file):
        commands.append(cmd)
        return 0

    monkeypatch.setattr(Analysis, "run_command", _record)
    return commands


@pytest.fixture
def published(monkeypatch):
    """Record the options of `aqua push` instead of publishing."""
    calls = []
    monkeypatch.setattr(push_analysis, "push_analysis", lambda **options: calls.append(options))
    return calls


@pytest.mark.parametrize(
    "project, args, label, exported",
    [
        ("alpha", [], "alpha/config.aqua-analysis.yaml", "alpha"),
        ("alpha", ["ensemble"], "alpha/config.aqua-analysis-ensemble.yaml", "alpha"),
        ("alpha", ["-p", "beta"], "beta/config.aqua-analysis.yaml", "beta"),
        ("alpha", ["ensemble", "--project", "beta"], "beta/config.aqua-analysis-ensemble.yaml", "beta"),
        ("alpha", ["-p", "beta", "--config", "CUSTOM"], "config.aqua-analysis.yaml", "beta"),
        (None, ["--config", "CUSTOM"], "config.aqua-analysis.yaml", None),
    ],
)
def test_analysis_project_config_and_env(
    analysis_project, recorded_commands, run_aqua, tmp_path, project, args, label, exported
):
    """Fails if the default config is not taken from the (selected) project folder, if `--config` does not win,
    or if AQUA_PROJECT and AQUA_PROJECT_PATH are not exported to the config (or are exported without project)."""
    site = analysis_project(project, folders=("analysis/alpha", "analysis/beta") if project else ())
    args = [str(tmp_path / "config.aqua-analysis.yaml") if arg == "CUSTOM" else arg for arg in args]

    run_aqua(["analysis", *args, "--serial", "-l", LOGLEVEL])

    assert len(recorded_commands) == 1
    config = recorded_commands[0].split(" --config ")[1].split(" -l ")[0]
    if exported is None:
        assert config == f"{label}|${{AQUA_PROJECT}}|${{AQUA_PROJECT_PATH}}"
    else:
        assert config == f"{label}|{exported}|{site / exported}"
        assert os.environ["AQUA_PROJECT"] == exported  # inherited by the diagnostic tools


PRE_PROJECT = "No project set in"
NO_FOLDER = "Cannot find the analysis folder for project"


@pytest.mark.parametrize(
    "project, folders, args, message",
    [
        (None, (), [], PRE_PROJECT),
        (None, (), ["ensemble"], PRE_PROJECT),
        (None, (), ["-p", "alpha", "--config", "CUSTOM"], PRE_PROJECT),
        ("alpha", ("analysis/alpha",), ["-p", "missing"], f"{NO_FOLDER} 'missing'"),
        ("alpha", (), [], f"{NO_FOLDER} 'alpha'"),  # as an installation of aqua-core alone
    ],
)
def test_analysis_project_errors(
    analysis_project, recorded_commands, run_aqua, aqua_caplog, tmp_path, project, folders, args, message
):
    """Fails if the default config or `-p` are accepted on an installation without project (with no pointer to
    `aqua install`), or if a project without analysis folder runs instead of stopping."""
    analysis_project(project, folders=folders)
    args = [str(tmp_path / "config.aqua-analysis.yaml") if arg == "CUSTOM" else arg for arg in args]
    caplog = aqua_caplog(logging.getLogger("AQUA Analysis"), logging.getLogger("AquaAnalysis"))

    with pytest.raises(SystemExit) as exc_info:
        run_aqua(["analysis", *args, "--serial", "-l", LOGLEVEL])

    assert exc_info.value.code == 1
    assert any(r.getMessage().startswith(message) for r in caplog.records)
    if message == PRE_PROJECT:
        assert any("run `aqua install` to upgrade it" in r.getMessage() for r in caplog.records)
    assert not recorded_commands


@pytest.mark.parametrize(
    "project, args, grouping",
    [
        ("alpha", [], "analysis/alpha/config.grouping.yaml"),
        ("alpha", ["-p", "beta"], "analysis/beta/config.grouping.yaml"),
        ("alpha", ["--project", "beta", "-c", "CUSTOM"], "config.grouping.yaml"),
        (None, ["-c", "CUSTOM"], "config.grouping.yaml"),
    ],
)
def test_push_grouping_file(analysis_project, published, run_aqua, tmp_path, project, args, grouping):
    """Fails if `aqua push` does not take config.grouping.yaml from the (selected) project folder or `-c` does not win."""
    analysis_project(project, folders=("analysis/alpha", "analysis/beta") if project else ())
    args = [str(tmp_path / "config.grouping.yaml") if arg == "CUSTOM" else arg for arg in args]

    assert run_aqua(["push", str(tmp_path / "output"), "catalog/model/exp/r1", *args]) == 0

    assert [options["config"] for options in published] == [str(tmp_path / grouping)]


@pytest.mark.parametrize(
    "project, folders, args, message",
    [
        (None, (), [], PRE_PROJECT),
        (None, (), ["-p", "alpha"], PRE_PROJECT),
        ("alpha", ("analysis/alpha",), ["-p", "missing"], f"{NO_FOLDER} 'missing'"),
        ("alpha", (), [], f"{NO_FOLDER} 'alpha'"),  # as an installation of aqua-core alone
    ],
)
def test_push_project_errors(analysis_project, published, run_aqua, aqua_caplog, tmp_path, project, folders, args, message):
    """Fails if `aqua push` (default grouping file or `-p`) runs without project support, or publishes with a missing
    grouping folder."""
    analysis_project(project, folders=folders)
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    assert run_aqua(["push", str(tmp_path / "output"), "catalog/model/exp/r1", *args]) == 1

    assert any(r.getMessage().startswith(message) for r in caplog.records)
    assert not published

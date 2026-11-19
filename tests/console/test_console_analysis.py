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
from aqua.core.console.main import AquaConsole
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
ANALYSIS_FILES = ("config.aqua-analysis.yaml", "config.aqua-analysis-ensemble.yaml", "config.grouping.yaml")


def _write_analysis_folder(folder, tmp_path, script):
    """Analysis folder whose configs run one tool with `--config <label>|${AQUA_PROJECT}|${AQUA_PROJECT_PATH}`.

    The label is the config path relative to the `analysis` folder, so the recorded command tells which config was used
    and the expanded variables tell what was exported.
    """
    os.makedirs(folder, exist_ok=True)
    label_root = os.path.join(tmp_path, "analysis")
    for name in ANALYSIS_FILES:
        path = os.path.join(folder, name)
        label = os.path.relpath(path, label_root) if path.startswith(label_root) else os.path.basename(path)
        dump_yaml(
            path,
            {
                "job": {
                    "catalog": "ci",
                    "model": "IFS",
                    "exp": "test",
                    "source": "lra",
                    "outputdir": str(tmp_path / "output"),
                    "report": False,
                },
                "cli": {"tool": str(script)},
                "run": [["collection"]],
                "diagnostics": {"collection": {"tool": {"config": f"{label}|${{AQUA_PROJECT}}|${{AQUA_PROJECT_PATH}}"}}},
            },
        )


@pytest.fixture
def project_layout(tmp_path, monkeypatch):
    """Return a function building a hand-made installation under tmp_path, found through AQUA_CONFIG.

    `nested`: active project `alpha`, analysis folders for `alpha` and `beta`, both with a package
    `aqua.<project>` under tmp_path (names of no real project, whose installed package would win).
    `legacy`: no project key (installation predating project support).
    `empty`: active project `alpha` without analysis folder (as an installation of aqua-core alone).
    """
    monkeypatch.delenv("AQUA_PROJECT", raising=False)
    monkeypatch.delenv("AQUA_PROJECT_PATH", raising=False)
    script = tmp_path / "cli_tool.py"
    script.touch()
    _write_analysis_folder(str(tmp_path), tmp_path, script)  # custom config, outside the analysis folder

    site = tmp_path / "site" / "aqua"
    for project in ("alpha", "beta"):
        (site / project).mkdir(parents=True)
        (site / project / "__init__.py").touch()
    monkeypatch.setattr(aqua, "__path__", [*aqua.__path__, str(site)])

    def _build(layout):
        config = {"machine": "ci", "catalog": None}
        if layout != "legacy":
            config["project"] = "alpha"
        if layout == "nested":
            for project in ("alpha", "beta"):
                _write_analysis_folder(os.path.join(tmp_path, "analysis", project), tmp_path, script)
        dump_yaml(os.path.join(tmp_path, "config-aqua.yaml"), config)
        monkeypatch.setenv("AQUA_CONFIG", str(tmp_path))
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


def _aqua(monkeypatch, args):
    """Run an AQUA console command in-process and return its exit status."""
    monkeypatch.setattr(sys, "argv", ["aqua", *args])
    return AquaConsole().execute()


@pytest.mark.parametrize(
    "layout, args, label, project",
    [
        ("nested", [], "alpha/config.aqua-analysis.yaml", "alpha"),
        ("nested", ["ensemble"], "alpha/config.aqua-analysis-ensemble.yaml", "alpha"),
        ("nested", ["-p", "beta"], "beta/config.aqua-analysis.yaml", "beta"),
        ("nested", ["ensemble", "--project", "beta"], "beta/config.aqua-analysis-ensemble.yaml", "beta"),
        ("nested", ["-p", "beta", "--config", "CUSTOM"], "config.aqua-analysis.yaml", "beta"),
        ("legacy", ["--config", "CUSTOM"], "config.aqua-analysis.yaml", None),
    ],
)
def test_analysis_project_config_and_env(
    project_layout, recorded_commands, monkeypatch, tmp_path, layout, args, label, project
):
    """Fails if the default config is not taken from the (selected) project folder, if `--config` does not win,
    or if AQUA_PROJECT and AQUA_PROJECT_PATH are not exported to the config (or are exported without project).

    Without project only an explicit `--config` runs: the default one is refused (see test_analysis_project_errors)."""
    site = project_layout(layout)
    args = [str(tmp_path / "config.aqua-analysis.yaml") if arg == "CUSTOM" else arg for arg in args]

    _aqua(monkeypatch, ["analysis", *args, "--serial", "-l", LOGLEVEL])

    assert len(recorded_commands) == 1
    config = recorded_commands[0].split(" --config ")[1].split(" -l ")[0]
    if project is None:
        assert config == f"{label}|${{AQUA_PROJECT}}|${{AQUA_PROJECT_PATH}}"
    else:
        assert config == f"{label}|{project}|{site / project}"
        # inherited by the diagnostic tools
        assert os.environ["AQUA_PROJECT"] == project


@pytest.mark.parametrize(
    "layout, args, message",
    [
        ("legacy", ["-p", "alpha"], "This installation predates project support (no project key in"),
        ("legacy", [], "No project set in"),
        ("legacy", ["ensemble"], "No project set in"),
        ("nested", ["-p", "missing"], "Cannot find the analysis folder for project 'missing'"),
        ("empty", [], "Cannot find the analysis folder for project 'alpha'"),
    ],
)
def test_analysis_project_errors(project_layout, recorded_commands, monkeypatch, aqua_caplog, layout, args, message):
    """Fails if `-p` or the default config are accepted on an installation without project (with no pointer to
    `aqua install`), or if a project without analysis folder runs instead of stopping."""
    project_layout(layout)
    caplog = aqua_caplog(logging.getLogger("AQUA Analysis"), logging.getLogger("AquaAnalysis"))

    with pytest.raises(SystemExit) as exc_info:
        _aqua(monkeypatch, ["analysis", *args, "--serial", "-l", LOGLEVEL])

    assert exc_info.value.code == 1
    assert any(r.getMessage().startswith(message) for r in caplog.records)
    if layout == "legacy":
        assert any("run `aqua install` to upgrade it" in r.getMessage() for r in caplog.records)
    assert not recorded_commands


@pytest.mark.parametrize(
    "layout, args, grouping",
    [
        ("nested", [], "analysis/alpha/config.grouping.yaml"),
        ("nested", ["-p", "beta"], "analysis/beta/config.grouping.yaml"),
        ("nested", ["--project", "beta", "-c", "CUSTOM"], "config.grouping.yaml"),
        ("legacy", ["-c", "CUSTOM"], "config.grouping.yaml"),
    ],
)
def test_push_grouping_file(project_layout, monkeypatch, tmp_path, layout, args, grouping):
    """Fails if `aqua push` does not take config.grouping.yaml from the (selected) project folder or `-c` does not win."""
    project_layout(layout)
    args = [str(tmp_path / "config.grouping.yaml") if arg == "CUSTOM" else arg for arg in args]
    published = []
    monkeypatch.setattr(push_analysis, "push_analysis", lambda **options: published.append(options))

    assert _aqua(monkeypatch, ["push", str(tmp_path / "output"), "catalog/model/exp/r1", *args]) == 0

    assert [options["config"] for options in published] == [str(tmp_path / grouping)]


@pytest.mark.parametrize(
    "layout, args, status, message",
    [
        ("legacy", ["-p", "alpha"], 2, "This installation predates project support (no project key in"),
        ("legacy", [], 1, "No project set in"),
        ("nested", ["-p", "missing"], 1, "Cannot find the analysis folder for project 'missing'"),
        ("empty", [], 1, "Cannot find the analysis folder for project 'alpha'"),
    ],
)
def test_push_project_errors(project_layout, monkeypatch, aqua_caplog, tmp_path, layout, args, status, message):
    """Fails if `aqua push` (default grouping file or `-p`) runs without project support, or publishes with a missing
    grouping folder."""
    project_layout(layout)
    caplog = aqua_caplog(logging.getLogger("AQUA"))
    published = []
    monkeypatch.setattr(push_analysis, "push_analysis", lambda **options: published.append(options))

    assert _aqua(monkeypatch, ["push", str(tmp_path / "output"), "catalog/model/exp/r1", *args]) == status

    assert any(r.getMessage().startswith(message) for r in caplog.records)
    assert not published

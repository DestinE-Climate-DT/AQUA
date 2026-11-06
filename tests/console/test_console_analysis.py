"""Console analysis workflow tests."""

from unittest.mock import patch

import pytest

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
        run_aqua(["add", "ci", "-e", catalog_copy_path])

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
                    "WARNING",
                ]
            )

        output_dir = tmp_path / "output" / "ci" / "ERA5" / "era5-hpz3" / "r1"
        assert output_dir.exists(), f"Output directory not created: {output_dir}"

        mock_tool.assert_called_once()

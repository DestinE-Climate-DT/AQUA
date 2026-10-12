"""Console analysis workflow tests."""

from unittest.mock import patch

import pytest

from aqua.core.util import dump_yaml

pytestmark = [pytest.mark.aqua, pytest.mark.console]


@pytest.mark.xdist_group(name="console_analysis_operations")
class TestAquaConsoleAnalysis:
    """Analysis operations against an isolated AQUA installation."""

    def test_console_analysis_checker_failure(self, aqua_install, run_aqua, tmp_path):
        """Test that a setup-checker failure is propagated through the console."""
        analysis_cfg = tmp_path / "config.aqua-analysis-test.yaml"
        dump_yaml(
            str(analysis_cfg),
            {
                "job": {
                    "run_checker": True,
                    "outputdir": str(tmp_path / "output"),
                    "catalog": "ci",
                    "model": "IFS",
                    "exp": "test-tco79",
                    "source": "lra-r100-monthly",
                },
            },
        )
        with patch("aqua.core.analysis.Analysis.run_setup_checker", side_effect=SystemExit(1)) as mock_checker:
            with pytest.raises(SystemExit) as exc_info:
                run_aqua(
                    [
                        "analysis",
                        "--config",
                        str(analysis_cfg),
                        "--checker",
                    ]
                )

        assert exc_info.value.code == 1
        mock_checker.assert_called_once_with()

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
                    "run_checker": False,
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

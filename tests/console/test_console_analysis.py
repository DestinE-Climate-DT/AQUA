"""Console analysis workflow tests."""

import logging
from unittest.mock import patch

import pytest

from aqua.core.util import dump_yaml

pytestmark = [pytest.mark.aqua, pytest.mark.console]


@pytest.mark.xdist_group(name="console_analysis_operations")
class TestAquaConsoleAnalysis:
    """Analysis operations against an isolated AQUA installation."""

    def test_console_analysis_checker(self, aqua_install, run_aqua, tmp_path, caplog):
        """Test that the analysis checker properly handles missing aqua.diagnostics package."""
        dummy_script = tmp_path / "cli_dummy.py"
        dummy_script.touch()

        diag_cfg = tmp_path / "diag_config.yaml"
        dump_yaml(str(diag_cfg), {"key": "value"})

        analysis_cfg = tmp_path / "config.aqua-analysis-test.yaml"
        dump_yaml(
            str(analysis_cfg),
            {
                "job": {
                    "run_checker": True,
                    "outputdir": str(tmp_path / "output"),
                    "model": "IFS",
                    "exp": "test-tco79",
                    "source": "lra-r100-monthly",
                },
            },
        )
        with pytest.raises(SystemExit) as exc_info:
            with caplog.at_level(logging.ERROR, logger="AquaAnalysis"):
                run_aqua(
                    [
                        "analysis",
                        "--config",
                        str(analysis_cfg),
                        "--checker",
                    ]
                )

        assert exc_info.value.code == 1

    def test_console_analysis_minimal(self, aqua_install, run_aqua, tmp_path):
        """Minimal smoke test: verifies aqua analysis routes through the config without
        running real subprocesses."""
        run_aqua(["add", "ci", "-e", "AQUA_tests/catalog_copy"])

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

"""Console analysis workflow tests."""

import json

import pytest
from utils_tests import LOGLEVEL

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

    @pytest.mark.parametrize("cli_source, default_source", [(None, "monthly"), ("short", "short")])
    def test_console_analysis_tool_sources(self, aqua_install, run_aqua, tmp_path, cli_source, default_source):
        """Tool sources override job/CLI defaults for every config without leaking into following tools."""
        dummy_script = tmp_path / "cli_dummy.py"
        dummy_script.write_text("import json, sys\nprint(json.dumps(sys.argv[1:]))\n")

        diag_cfg = tmp_path / "diag_config.yaml"
        dump_yaml(str(diag_cfg), {"key": "value"})
        other_cfg = tmp_path / "other_config.yaml"
        dump_yaml(str(other_cfg), {"key": "other"})

        analysis_cfg = tmp_path / "config.aqua-analysis-test.yaml"
        dump_yaml(
            str(analysis_cfg),
            {
                "job": {
                    "run_setup_checker": False,
                    "source": "monthly",
                    "outputdir": str(tmp_path / "output"),
                },
                "cli": dict.fromkeys(["two_d", "three_d", "default", "null_source"], str(dummy_script)),
                "run": [["dummy"]],
                "diagnostics": {
                    "dummy": {
                        "two_d": {"source": "original_2d", "config": [str(diag_cfg), str(other_cfg)]},
                        "three_d": {"source": "original_3d", "config": str(diag_cfg)},
                        "default": {"config": str(diag_cfg)},
                        "null_source": {"source": None, "config": str(diag_cfg)},
                    }
                },
            },
        )
        command = [
            "analysis", "--config", str(analysis_cfg), "--catalog", "ci",
            "-m", "ERA5", "-e", "era5-hpz3", "--serial", "-l", LOGLEVEL,
        ]
        if cli_source:
            command += ["--source", cli_source]
        run_aqua(command)

        output_dir = tmp_path / "output" / "ci" / "ERA5" / "era5-hpz3" / "r1"
        for logfile, source, config in [
            ("dummy-two_d-1.log", "original_2d", diag_cfg),
            ("dummy-two_d-2.log", "original_2d", other_cfg),
            ("dummy-three_d.log", "original_3d", diag_cfg),
            ("dummy-default.log", default_source, diag_cfg),
            ("dummy-null_source.log", default_source, diag_cfg),
        ]:
            args = json.loads((output_dir / logfile).read_text())
            assert args.count("--source") == 1
            assert args[args.index("--source") + 1] == source
            assert args[args.index("--config") + 1] == str(config)
            assert args[args.index("--model") + 1] == "ERA5"
            assert args[args.index("--exp") + 1] == "era5-hpz3"
            assert args[args.index("--catalog") + 1] == "ci"

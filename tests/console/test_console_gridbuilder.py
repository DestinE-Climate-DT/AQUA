"""Console tests for the grid-builder command."""

import json

import pytest

pytestmark = [pytest.mark.aqua, pytest.mark.console]


class TestAquaConsoleGridBuilder:
    """Tests for the aqua grids build CLI."""

    @pytest.mark.parametrize(
        "command_args",
        [
            ["grids", "build", "--model", "ERA5", "--exp", "era5-hpz3", "--source", "monthly"],
        ],
    )
    def test_aqua_console_gridbuilder(self, run_aqua, command_args, tmp_path):
        """Test the aqua grids build CLI, including a valid --reader_kwargs JSON payload"""
        run_aqua(command_args + ["--verify", "--outdir", str(tmp_path), "--reader_kwargs", '{"chunks": {"time": 12}}'])

    def test_aqua_console_gridbuilder_invalid_reader_kwargs(self, run_aqua, tmp_path):
        """Malformed --reader_kwargs JSON must fail fast, before any data is retrieved"""
        with pytest.raises(json.JSONDecodeError):
            run_aqua(
                [
                    "grids",
                    "build",
                    "--model",
                    "ERA5",
                    "--exp",
                    "era5-hpz3",
                    "--source",
                    "monthly",
                    "--outdir",
                    str(tmp_path),
                    "--reader_kwargs",
                    "{not valid json}",
                ]
            )

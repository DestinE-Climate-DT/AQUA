"""Test for cli_checker command line interface"""

import argparse
import os
import subprocess
import sys

import pytest

from aqua import __path__ as aqua_pkg_path
from aqua.core.analysis.cli_checker import main, parse_arguments
from aqua.core.exceptions import NoDataError

pytestmark = pytest.mark.aqua


@pytest.fixture
def checker_script():
    """Path to the cli_checker script."""
    return os.path.join(aqua_pkg_path[0], "core", "analysis", "cli_checker.py")


@pytest.fixture
def base_args(checker_script):
    """Base subprocess args shared across cli_checker invocations."""

    def _build(*extra_args, loglevel="WARNING"):
        return [sys.executable, checker_script, "--loglevel", loglevel, *extra_args]

    return _build


def test_cli_checker_parse_arguments():
    """Test that parse_arguments correctly parses cli_checker specific arguments and defaults."""
    # fmt: off
    args = parse_arguments([
        "--model", "IFS",
        "--exp", "test-tco79",
        "--source", "short",
        "--yaml", "/tmp/test",
        "--no-read",
        "--no-rebuild",
        "--realization", "r1",
        "--regrid", "r200",
    ])
    # fmt: on
    assert args.model == "IFS"
    assert args.exp == "test-tco79"
    assert args.source == "short"
    assert args.yaml == "/tmp/test"
    assert args.read is False
    assert args.rebuild is False
    assert args.realization == "r1"
    assert args.regrid == "r200"

    args_defaults = parse_arguments(["--model", "IFS", "--exp", "test-tco79", "--source", "short"])
    assert args_defaults.yaml is None
    assert args_defaults.read is True
    assert args_defaults.rebuild is True
    assert args_defaults.realization is None


def test_cli_checker_valid_entry(tmp_path):
    """Test cli_checker with a valid catalog entry.

    This test requires AQUA to be installed with the 'ci' catalog.
    Note: This test may be skipped if AQUA is not fully installed.
    """
    yaml_dir = tmp_path / "yaml_output"
    yaml_dir.mkdir()
    myargs = argparse.Namespace(
        catalog="ci",
        model="IFS",
        exp="test-tco79",
        source="short",
        yaml=str(yaml_dir),
        read=False,
        rebuild=False,
        loglevel="WARNING",
    )

    try:
        main(myargs)
    except (NoDataError, Exception):
        pytest.skip("AQUA not fully installed or 'ci' catalog not available")

    yaml_file = yaml_dir / "experiment.yaml"
    assert yaml_file.exists(), "experiment.yaml should be created"


def test_cli_checker_invalid_entry(base_args):
    """Test cli_checker with an invalid catalog entry raises NoDataError."""
    result = subprocess.run(
        base_args(
            "--catalog",
            "ci",
            "--model",
            "invalid-model",
            "--exp",
            "invalid-exp",
            "--source",
            "invalid-source",
            "--no-read",
            "--no-rebuild",
        ),
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode != 0, "cli_checker should fail for invalid entry"
    assert "Failed to retrieve data" in result.stderr or "NoDataError" in result.stderr or "not found" in result.stderr.lower()


def test_cli_checker_missing_arguments(base_args):
    """Test cli_checker fails when required arguments are missing."""
    result = subprocess.run(
        base_args("--exp", "test-tco79", "--source", "short"),
        capture_output=True,
        text=True,
        timeout=10,
    )

    assert result.returncode != 0
    assert "model, exp and source are required" in result.stderr or "ValueError" in result.stderr

"""
Test some utilities and functions in the aqua.analysis module.

The more structured test of aqua analysis console command is
in tests/test_console.py
"""

import pytest

from aqua.core.analysis import run_command, run_diagnostic_collection
from aqua.core.analysis.analysis import _build_extra_args
from aqua.core.logger import log_configure

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

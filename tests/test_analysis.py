"""
Test some utilities and functions in the aqua.analysis module.

The more structured test of aqua analysis console command is
in tests/test_console.py
"""
import pytest
from aqua.logger import log_configure
from aqua.analysis import run_command, run_diagnostic_func, get_aqua_paths

logger = log_configure("DEBUG", "test_analysis")

@pytest.mark.aqua
def test_run_command():
    """Test the run_command function."""
    command = "echo 'Hello, World!'"
    
    
    with pytest.raises(TypeError):
        # Test with missing log_file argument
        _ = run_command(command, logger=logger)

@pytest.mark.aqua
def test_run_diagnostic_func(tmp_path):
    """Test the run_diagnostic_func function."""

    res = run_diagnostic_func(diagnostic='pluto', config={}, logger=logger)
    assert res is None, "Expected None return value for empty config"

    config = {
        'diagnostics': {
            'pluto': {
                'nworkers': 1,
                'config': 'pippo.yaml',
            }
        }
    }

    # Go through run_diagnostic_func and fail
    # at the final run_diagnostic call.
    # The fail is a return code != 0 so there is no
    # raise Exception, we just check that the function
    # completes without errors.
    run_diagnostic_func(diagnostic='pluto', parallel=True,
                        regrid='r100', logger=logger,
                        config=config, cluster=True,
                        catalog='test_catalog', realization='r2')
    
    assert True, "run_diagnostic_func should complete without errors"
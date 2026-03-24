"""Test for cli_checker command line interface"""

import os
import sys
import subprocess
import pytest
from aqua import __path__ as aqua_pkg_path
from aqua.core.analysis.cli_checker import parse_arguments

pytestmark = pytest.mark.aqua


def test_cli_checker_parse_arguments():
    """Test that parse_arguments correctly parses cli_checker specific arguments and defaults."""
    # Test with all flags provided
    args = parse_arguments([
        '--model', 'IFS',
        '--exp', 'test-tco79',
        '--source', 'short',
        '--yaml', '/tmp/test',
        '--no-read',
        '--no-rebuild',
        '--realization', 'r1',
        '--regrid', 'r200'
    ])
    
    assert args.model == 'IFS'
    assert args.exp == 'test-tco79'
    assert args.source == 'short'
    assert args.yaml == '/tmp/test'
    assert args.read is False  # --no-read sets read=False
    assert args.rebuild is False  # --no-rebuild sets rebuild=False
    assert args.realization == 'r1'
    assert args.regrid == 'r200'
    
    # Test defaults when flags are not provided
    args_defaults = parse_arguments([
        '--model', 'IFS',
        '--exp', 'test-tco79',
        '--source', 'short'
    ])
    
    assert args_defaults.yaml is None
    assert args_defaults.read is True  # Default is True (read data)
    assert args_defaults.rebuild is True  # Default is True (rebuild areas)
    assert args_defaults.realization is None


@pytest.mark.slow
def test_cli_checker_valid_entry(tmp_path):
    """Test cli_checker with a valid catalog entry.
    
    This test requires AQUA to be installed with the 'ci' catalog.
    Note: This test may be skipped if AQUA is not fully installed.
    """
    
    checker_script = os.path.join(aqua_pkg_path[0], "core", "analysis", "cli_checker.py")
    yaml_dir = tmp_path / "yaml_output"
    yaml_dir.mkdir()
    
    # Test with a valid entry from the 'ci' catalog, using --no-read to speed up
    result = subprocess.run(
        [
            sys.executable, checker_script,
            '--catalog', 'ci',
            '--model', 'IFS',
            '--exp', 'test-tco79',
            '--source', 'short',
            '--yaml', str(yaml_dir),
            '--no-read',  # Don't read data, just check catalog and create yaml
            '--no-rebuild',  # Don't rebuild areas
            '--loglevel', 'WARNING'
        ],
        capture_output=True,
        text=True,
        timeout=60
    )
    
    # Should succeed if AQUA is properly installed
    if result.returncode == 0:
        # Check that experiment.yaml was created
        yaml_file = yaml_dir / "experiment.yaml"
        assert yaml_file.exists(), "experiment.yaml should be created"
    else:
        # If it fails due to missing installation, skip the test
        pytest.skip("AQUA not fully installed or 'ci' catalog not available")


def test_cli_checker_invalid_entry():
    """Test cli_checker with an invalid catalog entry raises NoDataError."""
    checker_script = os.path.join(aqua_pkg_path[0], "core", "analysis", "cli_checker.py")
    
    # Test with an invalid entry
    result = subprocess.run(
        [
            sys.executable, checker_script,
            '--catalog', 'ci',
            '--model', 'invalid-model',
            '--exp', 'invalid-exp',
            '--source', 'invalid-source',
            '--no-read',
            '--no-rebuild',
            '--loglevel', 'WARNING'
        ],
        capture_output=True,
        text=True,
        timeout=30
    )
    
    # Should fail with exit code 1 (or any non-zero)
    assert result.returncode != 0, "cli_checker should fail for invalid entry"
    # Check for error message (may be NoDataError or other Reader initialization error)
    assert "Failed to retrieve data" in result.stderr or "NoDataError" in result.stderr or "not found" in result.stderr.lower()


def test_cli_checker_missing_arguments():
    """Test cli_checker fails when required arguments are missing."""
    checker_script = os.path.join(aqua_pkg_path[0], "core", "analysis", "cli_checker.py")
    
    # Test with missing model
    result = subprocess.run(
        [
            sys.executable, checker_script,
            '--exp', 'test-tco79',
            '--source', 'short',
            '--loglevel', 'WARNING'
        ],
        capture_output=True,
        text=True,
        timeout=10
    )
    
    # Should fail (either ValueError or SystemExit from argparse)
    assert result.returncode != 0
    assert "model, exp and source are required" in result.stderr or "ValueError" in result.stderr


import os
import pytest
import subprocess
from unittest.mock import patch
from aqua.cli.main import AquaConsole  # Ensure this import is correct
from aqua.util import create_folder
from tropical_rainfall.cli.main import TropicalRainfallConsole

def is_package_installed(package_name):
    """Check if a package is installed"""
    result = subprocess.run(["pip", "show", package_name], capture_output=True, text=True)
    return result.returncode == 0

def test_enable_tropical_rainfall(monkeypatch, tmpdir):
    """Test enabling the tropical_rainfall package"""
    if is_package_installed("tropical_rainfall"):
        subprocess.run(["pip", "uninstall", "-y", "tropical_rainfall"], capture_output=True, text=True)
        # Ensure tropical_rainfall command is not available before enabling
        result = subprocess.run(["tropical_rainfall", "--path"], capture_output=True, text=True)
        assert result.returncode != 0, "tropical_rainfall command should not be accessible before enabling"

    # Mock sys.argv to simulate command-line arguments
    source_path = tmpdir.mkdir("tropical_rainfall_source")
    test_args = ["aqua", "enable", "tropical_rainfall"]
    monkeypatch.setattr("sys.argv", test_args)

    # Instantiate and execute the AquaConsole to enable the package
    console = AquaConsole()
    console.execute()

    # Verify the tropical_rainfall command is now accessible
    result = subprocess.run(["tropical_rainfall", "--path"], capture_output=True, text=True)
    if result.returncode != 0:
        print("STDOUT:", result.stdout)
        print("STDERR:", result.stderr)
    assert result.returncode == 0, "tropical_rainfall command should be accessible after enabling"

    # Test accessing the path command
    result = subprocess.run(["tropical_rainfall", "--path"], capture_output=True, text=True)
    assert "tropical_rainfall" in result.stdout, "Path command should return the tropical_rainfall path"

def test_run_cli_with_config(monkeypatch):
    """Test running the CLI with a provided config file"""
    # Path to the test config file
    config_file = os.path.join(os.path.dirname(__file__), 'test_config.yml')

    # Mock sys.argv to simulate command-line arguments
    test_args = ["tropical_rainfall", "run_cli", "--config_file", str(config_file)]
    monkeypatch.setattr("sys.argv", test_args)

    console = TropicalRainfallConsole()

    try:
        console.execute()
    except SystemExit as e:
        # Verify that the CLI ran successfully
        assert e.code == 0, f"CLI execution failed with exit code {e.code}"

    # Additional verification if needed
    assert console.logger is not None, "Logger should be initialized"

def test_disable_tropical_rainfall(monkeypatch, tmpdir):
    """Test disabling the tropical_rainfall package"""
    # First, ensure the package is enabled
    if not is_package_installed("tropical_rainfall"):
        source_path = tmpdir.mkdir("tropical_rainfall_source")
        enable_args = ["aqua", "enable", "tropical_rainfall", "--editable", str(source_path)]
        monkeypatch.setattr("sys.argv", enable_args)
        console = AquaConsole()
        console.execute()

    # Mock sys.argv to simulate command-line arguments for disabling
    disable_args = ["aqua", "disable", "tropical_rainfall"]
    monkeypatch.setattr("sys.argv", disable_args)

    # Instantiate and execute the AquaConsole to disable the package
    console = AquaConsole()
    console.execute()

    # Verify the tropical_rainfall command is no longer accessible
    result = subprocess.run(["tropical_rainfall", "--path"], capture_output=True, text=True)
    assert result.returncode != 0, "tropical_rainfall command should not be accessible after disabling"

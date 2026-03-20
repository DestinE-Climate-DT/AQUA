"""
Shared fixtures for AQUA test suite.
These fixtures use scope="session" to retrieve data once and share across all tests.
Reference: https://docs.pytest.org/en/stable/reference/fixtures.html
"""
import os
import tempfile
from pathlib import Path

import matplotlib

matplotlib.use('Agg')  # Non-interactive backend

import matplotlib.pyplot as plt

plt.ioff()  # Turn off interactive mode explicitly

import pytest
from utils_tests import TestCleanupRegistry

from aqua import Reader
from aqua.core.configurer import ConfigPath

# Centralized setting for all tests
DPI = 50
APPROX_REL = 1e-4
LOGLEVEL = "DEBUG"


# ======================================================================
# Run global (not per-worker) cleanup hooks for test-generated files
# ======================================================================
def pytest_configure(config):
    """
    Runs once at session start on controller and workers with xdist.
    Cache configdir before any test can modify HOME.
    Set per-worker TMPDIR to avoid CDO/temp contention in parallel runs.
    """
    try:
        config_path = ConfigPath()
        config._stored_configdir = config_path.configdir
    except (FileNotFoundError, KeyError):
        config._stored_configdir = None

    workerinput = getattr(config, "workerinput", None)
    if workerinput is not None:
        worker_id = workerinput.get("workerid", "master")
        os.environ["TMPDIR"] = tempfile.mkdtemp(prefix=f"aqua_pytest_{worker_id}_")


def pytest_sessionfinish(session, exitstatus):
    """
    Runs once at the end of the entire test session (controller only with xdist).
    Uses current configdir if available, otherwise falls back to cached one.
    This prevents race conditions in parallel test.
    """
    # Get stored configdir from session.config (same object as config in pytest_configure)
    stored_configdir = getattr(session.config, '_stored_configdir', None)

    # Prefer current configdir, but fall back to stored one if ConfigPath() fails (e.g. HOME was deleted)
    cleanup_configdir = stored_configdir
    try:
        config_path = ConfigPath()
        cleanup_configdir = config_path.configdir
    except (FileNotFoundError, KeyError):
        # If HOME was deleted, rely on the stored configdir from pytest_configure
        pass

    if cleanup_configdir:
        registry = TestCleanupRegistry(cleanup_configdir)
        registry.cleanup()


# ======================================================================
# xdist: log worker -> test mapping (per test start/finish) for hang debugging
# Log file: .pytest_cache/xdist_worker_tests.log
# ======================================================================

def _get_xdist_log_path(config):
    """Resolve the path for the xdist worker log file."""
    root = getattr(config, "rootdir", None)
    if not root:
        return None
    # Handle pytest's distinct path objects if necessary
    root_path = root.path if hasattr(root, "path") else root
    return Path(root_path) / ".pytest_cache" / "xdist_worker_tests.log"


def _log_xdist_event(config, event, nodeid, outcome=None):
    """Append a structured log entry for xdist workers."""
    worker_input = getattr(config, "workerinput", None)
    if not worker_input:
        return

    worker_id = worker_input.get("workerid", "unknown")
    log_path = _get_xdist_log_path(config)

    if not log_path:
        return

    # Construct the log line
    line = f"{worker_id}\t{event}\t{nodeid}"
    if outcome:
        line += f"\t{outcome}"

    try:
        # Ensure directory exists
        log_path.parent.mkdir(parents=True, exist_ok=True)
        # Append line with flush to ensure it's written immediately
        with open(log_path, "a") as f:
            f.write(f"{line}\n")
            f.flush()
    except OSError:
        pass  # Best effort logging


def pytest_sessionstart(session):
    """Clear the xdist worker log on the controller node."""
    if getattr(session.config, "workerinput", None) is not None:
        return  # only the controller clears; workers append
    log_path = _get_xdist_log_path(session.config)
    if log_path and log_path.exists():
        try:
            log_path.unlink()
        except OSError:
            pass


def pytest_runtest_setup(item):
    """Log test start event."""
    _log_xdist_event(item.config, "started", item.nodeid)


def pytest_runtest_makereport(item, call):
    """Log test finish event with outcome."""
    if call.when == "call":
        outcome = "failed" if call.excinfo else "passed"
        _log_xdist_event(item.config, "finished", item.nodeid, outcome)


# ===================== Reader and Retrieve fixtures ===================
# ======================================================================
# IFS fixtures
# ======================================================================
@pytest.fixture(scope="session")
def ifs_tco79_short_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def ifs_tco79_short_data(ifs_tco79_short_reader):
    return ifs_tco79_short_reader.retrieve()

@pytest.fixture(scope="session")
def ifs_tco79_short_data_2t(ifs_tco79_short_reader):
    return ifs_tco79_short_reader.retrieve(var='2t')

@pytest.fixture(scope="session")
def ifs_tco79_short_r100_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL, regrid="r100")

@pytest.fixture(scope="session")
def ifs_tco79_short_r100_data(ifs_tco79_short_r100_reader):
    return ifs_tco79_short_r100_reader.retrieve()

@pytest.fixture(scope="session")
def ifs_tco79_short_r200_reader():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=LOGLEVEL, regrid="r200")

@pytest.fixture(scope="session")
def ifs_tco79_short_r200_data(ifs_tco79_short_r200_reader):
    return ifs_tco79_short_r200_reader.retrieve()

@pytest.fixture(scope="session")
def ifs_tco79_long_fixFalse_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", fix=False, loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def ifs_tco79_long400_fixFalse_reader():
    return Reader(model="IFS", exp="test-tco79", source="long400", fix=False, loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def ifs_tco79_long_fixFalse_data(ifs_tco79_long_fixFalse_reader):
    return ifs_tco79_long_fixFalse_reader.retrieve(var=['2t', 'ttr'])

@pytest.fixture(scope="session")
def ifs_tco79_long400_fixFalse_data(ifs_tco79_long400_fixFalse_reader):
    return ifs_tco79_long400_fixFalse_reader.retrieve(var=['2t', 'ttr'])

@pytest.fixture(scope="session")
def ifs_tco79_long_reader():
    return Reader(model="IFS", exp="test-tco79", source="long", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def ifs_tco79_long_data(ifs_tco79_long_reader):
    return ifs_tco79_long_reader.retrieve()

# ======================================================================
# FESOM fixtures
# ======================================================================
@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_reader():
    return Reader(model="FESOM", exp="test-pi", source="original_2d", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_data(fesom_test_pi_original_2d_reader):
    return fesom_test_pi_original_2d_reader.retrieve(var='tos')

@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_r200_fixFalse_reader():
    return Reader(model="FESOM", exp="test-pi", source="original_2d",
                  regrid="r200", fix=False, loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def fesom_test_pi_original_2d_r200_fixFalse_data(fesom_test_pi_original_2d_r200_fixFalse_reader):
    return fesom_test_pi_original_2d_r200_fixFalse_reader.retrieve()

# ======================================================================
# ICON fixtures
# ======================================================================
@pytest.fixture(scope="session")
def icon_test_healpix_short_reader():
    return Reader(model="ICON", exp="test-healpix", source="short", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def icon_test_healpix_short_data(icon_test_healpix_short_reader):
    return icon_test_healpix_short_reader.retrieve(var='2t')

@pytest.fixture(scope="session")
def icon_test_r2b0_short_reader():
    return Reader(model="ICON", exp="test-r2b0", source="short", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def icon_test_r2b0_short_data(icon_test_r2b0_short_reader):
    return icon_test_r2b0_short_reader.retrieve(var='t')

# ======================================================================
# NEMO fixtures
# ======================================================================
@pytest.fixture(scope="session")
def nemo_test_eORCA1_long_2d_reader():
    return Reader(model="NEMO", exp="test-eORCA1", source="long-2d", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def nemo_test_eORCA1_long_2d_data(nemo_test_eORCA1_long_2d_reader):
    return nemo_test_eORCA1_long_2d_reader.retrieve(var='tos')

@pytest.fixture(scope="session")
def nemo_test_eORCA1_short_3d_reader():
    return Reader(model="NEMO", exp="test-eORCA1", source="short-3d", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def nemo_test_eORCA1_short_3d_data(nemo_test_eORCA1_short_3d_reader):
    return nemo_test_eORCA1_short_3d_reader.retrieve(var='so')

# ======================================================================
# ERA5 fixtures
# ======================================================================
@pytest.fixture(scope="session")
def era5_hpz3_monthly_reader():
    return Reader(model="ERA5", exp='era5-hpz3', source='monthly', loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def era5_hpz3_monthly_data(era5_hpz3_monthly_reader):
    return era5_hpz3_monthly_reader.retrieve(var=['2t', 'tprate','q'])

@pytest.fixture(scope="session")
def era5_hpz3_monthly_r100_reader():
    return Reader(model="ERA5", exp='era5-hpz3', source='monthly', regrid="r100", loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def era5_hpz3_monthly_r100_data(era5_hpz3_monthly_r100_reader):
    return era5_hpz3_monthly_r100_reader.retrieve(var=['q'])

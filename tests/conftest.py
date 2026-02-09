"""
Shared fixtures for AQUA test suite.
These fixtures use scope="session" to retrieve data once and share across all tests.
Reference: https://docs.pytest.org/en/stable/reference/fixtures.html
"""
import matplotlib
matplotlib.use('Agg')  # Non-interactive backend

import matplotlib.pyplot as plt
plt.ioff()  # Turn off interactive mode explicitly

import pytest
from aqua import Reader
from aqua.core.configurer import ConfigPath
from utils_tests import TestCleanupRegistry

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
    """
    try:
        config_path = ConfigPath()
        config._stored_configdir = config_path.configdir
    except (FileNotFoundError, KeyError):
        config._stored_configdir = None


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
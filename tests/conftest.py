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
# Cleanup fixture for test-generated files
# ======================================================================
@pytest.fixture(scope="session", autouse=True)
def cleanup_test_files():
    """
    Session-scoped fixture that automatically cleans up test-generated files.
    This prevents race conditions in parallel test execution where:
    - One test creates a file (e.g., nemo-curvilinear.yaml)
    - Another test tries to read it while the first test is deleting it
    """
    # Store configdir at session start (before any tests modify environment)
    # This ensures cleanup can run even if HOME is deleted during tests
    try:
        config_path = ConfigPath()
        stored_configdir = config_path.configdir
    except (FileNotFoundError, KeyError):
        stored_configdir = None
    
    # Run all tests first
    yield
    
    # Cleanup after all tests complete
    # Try to get configdir again, but fall back to stored value if ConfigPath fails
    cleanup_configdir = stored_configdir
    try:
        config_path = ConfigPath()
        cleanup_configdir = config_path.configdir
    except (FileNotFoundError, KeyError):
        # If ConfigPath fails (e.g., HOME deleted), use stored configdir
        # This handles the test_console_without_home case
        pass
    
    # Only attempt cleanup if we have a valid configdir
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
def ifs_tco79_long_fixFalse_data(ifs_tco79_long_fixFalse_reader):
    return ifs_tco79_long_fixFalse_reader.retrieve(var=['2t', 'ttr'])

@pytest.fixture(scope="session")
def ifs_tco79_long_cftime_fixFalse_reader():
    return Reader(model="IFS", exp="test-tco79", source="long-cftime", fix=False, loglevel=LOGLEVEL)

@pytest.fixture(scope="session")
def ifs_tco79_long_cftime_fixFalse_data(ifs_tco79_long_cftime_fixFalse_reader):
    return ifs_tco79_long_cftime_fixFalse_reader.retrieve(var=['2t', 'ttr'])

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
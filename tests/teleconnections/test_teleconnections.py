import pytest
import sys
sys.path.insert(1, './diagnostics/teleconnections/')
from teleconnections.cdo_testing import cdo_station_based_comparison
from teleconnections.cdo_testing import cdo_regional_mean_comparison
from teleconnections.tools import load_namelist, lon_180_to_360


# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.parametrize("module_name", ['cdo_testing', 'index', 'plots',
                                         'tools'])
@pytest.mark.teleconnections
def test_import(module_name):
    """
    """
    try:
        __import__(module_name)
    except ImportError:
        assert False, "Module {} could not be imported".format(module_name)


@pytest.mark.teleconnections
def test_lon_conversion():
    """
    Test that the lon conversion works
    """
    assert lon_180_to_360(-25) == pytest.approx(335, rel=approx_rel)
    assert lon_180_to_360(-75) == pytest.approx(285, rel=approx_rel)
    assert lon_180_to_360(25) == pytest.approx(25, rel=approx_rel)
    assert lon_180_to_360(75) == pytest.approx(75, rel=approx_rel)


@pytest.mark.teleconnections
def test_namelist():
    """
    Test that the namelist can be loaded
    """
    configdir = "./diagnostics/teleconnections/config"
    diagname = 'teleconnections'
    namelist = load_namelist(diagname, configdir=configdir)
    assert len(namelist) > 0


@pytest.mark.teleconnections
@pytest.mark.parametrize("months_window", [1, 3])
def test_station_based(months_window, loglevel=loglevel):
    """
    Test that the station_based method works
    """
    filepath = "./nao_test.nc"
    configdir = "./diagnostics/teleconnections/config"
    diagname = 'teleconnections'
    telecname = 'NAO'
    rtol = approx_rel
    atol = approx_rel

    # 1. -- Opening yml files
    namelist = load_namelist(diagname, configdir=configdir)

    # 2. -- Comparison cdo vs lib method
    cdo_station_based_comparison(infile=filepath, namelist=namelist,
                                 telecname=telecname, rtol=rtol, atol=atol,
                                 months_window=months_window,
                                 loglevel=loglevel)


@pytest.mark.teleconnections
@pytest.mark.parametrize("months_window", [1, 3])
def test_regional_mean(months_window):
    """
    Test that the regional_mean method works
    """
    filepath = "./enso_test.nc"
    configdir = "./diagnostics/teleconnections/config"
    diagname = 'teleconnections'
    telecname = 'ENSO'
    rtol = approx_rel
    atol = approx_rel

    # 1. -- Opening yml files
    namelist = load_namelist(diagname, configdir=configdir)

    # 2. -- Comparison cdo vs lib method
    cdo_regional_mean_comparison(infile=filepath, namelist=namelist,
                                 telecname=telecname, rtol=rtol, atol=atol,
                                 months_window=months_window,
                                 loglevel=loglevel)
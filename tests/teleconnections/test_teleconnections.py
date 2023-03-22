import numpy as np
import pytest
import sys
from aqua import Reader
sys.path.insert(1, './diagnostics/teleconnections/')
from cdotesting import cdo_station_based_comparison, cdo_regional_mean_comparison
from tools import load_namelist, lon_180_t0_360

# pytest approximation, to bear with different machines
approx_rel=1e4

@pytest.mark.parametrize("module_name", ['cdotesting', 'index', 'plots', 'tools'])
def test_import(module_name):
    """
    Test that the module can be imported
    """
    try:
        __import__(module_name)
    except ImportError:
        assert False, "Module {} could not be imported".format(module_name)

def test_lon_conversion():
    """
    Test that the lon conversion works
    """
    assert lon_180_t0_360(-25) == pytest.approx(335, rel=approx_rel)
    assert lon_180_t0_360(-75) == pytest.approx(285, rel=approx_rel)
    assert lon_180_t0_360(25)  == pytest.approx(25, rel=approx_rel)
    assert lon_180_t0_360(75)  == pytest.approx(75, rel=approx_rel)

def test_namelist():
    """
    Test that the namelist can be loaded
    """
    configdir = "./diagnostics/teleconnections/"
    diagname  = 'teleconnections'
    namelist = load_namelist(diagname,configdir=configdir)
    assert len(namelist) > 0

@pytest.mark.parametrize("months_window", [1,3])
def test_station_based(months_window):
    """
    Test that the station_based method works
    """
    filepath = "./nao_test.nc"
    configdir = "./diagnostics/teleconnections/"
    diagname  = 'teleconnections'
    telecname = 'NAO'
    rtol = 1e-4
    atol = 1e-4

    # 1. -- Opening yml files
    namelist = load_namelist(diagname,configdir=configdir)

    # 2. -- Comparison cdo vs lib method
    cdo_station_based_comparison(infile=filepath,namelist=namelist,
                                 telecname=telecname,rtol=rtol,atol=atol,
                                 months_window=months_window)

@pytest.mark.parametrize("months_window", [1,3])
def test_regional_mean(months_window):
    """
    Test that the regional_mean method works
    """
    filepath = "./enso_test.nc"
    configdir = "./diagnostics/teleconnections/"
    diagname  = 'teleconnections'
    telecname = 'ENSO'
    rtol = 1e-4
    atol = 1e-4

    # 1. -- Opening yml files
    namelist = load_namelist(diagname,configdir=configdir)

    # 2. -- Comparison cdo vs lib method
    cdo_regional_mean_comparison(infile=filepath,namelist=namelist,
                                 telecname=telecname,rtol=rtol,atol=atol,
                                 months_window=months_window)
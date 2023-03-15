import pytest
import sys
sys.path.insert(1, '../../')
from aqua import Reader
sys.path.insert(1, '../../diagnostics/teleconnections/')
from cdotesting import *
from tools import *

@pytest.mark.parametrize("module_name", ['cdotesting', 'index', 'plots', 'tools'])
def test_import(module_name):
    """
    Test that the module can be imported
    """
    try:
        __import__(module_name)
    except ImportError:
        assert False, "Module {} could not be imported".format(module_name)

@pytest.mark.parametrize("months_window", [1,3])
def test_regional_mean(months_window):
    """
    Test that the regional_mean method works
    """
    filepath = "../../diagnostics/teleconnections/data/enso_test.nc"
    configdir = "../../diagnostics/teleconnections/"
    diagname  = 'teleconnections'
    telecname = 'ENSO'

    # Opening yml files
    namelist = load_namelist(diagname,configdir=configdir)
    test = regional_mean_cdo(filepath,namelist,telecname,months_window=months_window)
    assert len(test) > 0
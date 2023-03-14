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


# @pytest.fixture
# def reader_instance():
#     """
#     Create a reader instance
#     """
#     return Reader(model="IFS", exp="test-tco2559", source="ICMGG_atm2d", regrid="r200")

# def test_retrieve(reader_instance):
#     """
#     Test that the retrieve method works
#     """
#     data = reader_instance.retrieve(fix=False)
#     print(data)
#     assert len(data) > 0

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
    test = station_based_cdo(filepath,namelist,telecname,months_window=months_window)
    assert len(test) > 0
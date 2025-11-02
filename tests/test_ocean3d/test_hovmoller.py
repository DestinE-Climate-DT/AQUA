import pytest
from aqua.diagnostics.ocean_drift import Hovmoller
from conftest import LOGLEVEL

loglevel = LOGLEVEL


@pytest.mark.diagnostics
def test_hovmoller():
    """Test the Hovmoller class."""
    # Create an instance of the Hovmoller class
    hovmoller = Hovmoller(catalog='ci', model='FESOM',
                          exp='hpz3', source='monthly-3d',
                          startdate='1990-01-01', enddate='1990-03-31',
                          regrid='r200', loglevel=loglevel)
    
    hovmoller.run(region='sss')

    assert hovmoller is not None, "Hovmoller instance should not be None"
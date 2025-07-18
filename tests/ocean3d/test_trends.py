import pytest
from aqua.diagnostics.ocean_trends import Trends



@pytest.mark.diagnostics
def test_trends():
    """Test the trends class."""
    # Create an instance of the trends class
    trend = Trends(catalog='ci', model='FESOM',
                          exp='hpz3', source='monthly-3d',
                          regrid='r100', loglevel='DEBUG')
    
    trend.run(dim_mean="lat",
              var=['thetao', 'so'],
              region='io')

    assert trend is not None, "trend instance should not be None"
    assert trend.trend_coef.thetao.isel(level=0).mean('lon').values == pytest.approx(-0.06689141, rel=1e-1), "Coefficient of trend should be approximately zero"
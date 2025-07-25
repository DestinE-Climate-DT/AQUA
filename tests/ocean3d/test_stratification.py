import pytest
from aqua.diagnostics.ocean_stratification.stratification import Stratification

approx_rel = 1e-3

@pytest.mark.diagnostics
def test_stratification():
    """Test the stratification class."""
    # Create an instance of the stratification class
    strat = Stratification(catalog='ci', model='FESOM',
                          exp='hpz3', source='monthly-3d',
                          regrid='r100', loglevel='DEBUG')

    strat.run(
        # dim_mean="lat",
              var=['thetao', 'so'],
              region='Labrador Sea',
              mld=True,
              )
    data = strat.data.sel(month=12)
    assert strat is not None, "strat instance should not be None"
    assert data["mld"].isel(lon=10, lat=10).values == pytest.approx(-0.06603967,rel=approx_rel)
    assert data["rho"].isel(lev=5).values == pytest.approx(0.02622599,rel=approx_rel)

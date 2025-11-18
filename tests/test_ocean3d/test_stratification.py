import pytest
from aqua.diagnostics.ocean_stratification.stratification import Stratification
from conftest import APPROX_REL, LOGLEVEL

loglevel = LOGLEVEL
approx_rel = APPROX_REL*10

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
pytestmark = [
    pytest.mark.diagnostics,
    pytest.mark.xdist_group(name="dask_operations")
]

def test_stratification():
    """Test the stratification class."""
    # Create an instance of the stratification class
    strat = Stratification(catalog='ci', model='FESOM',
                          exp='hpz3', source='monthly-3d',
                          regrid='r100', loglevel=loglevel)

    strat.run(
        dim_mean=["lat","lon"],
        var=['thetao', 'so'],
        climatology='January',
        region='ls',
        mld=True,
        )
    assert strat is not None, "strat instance should not be None"
    assert strat.data["mld"].values == pytest.approx(2.5000076,rel=approx_rel)
    assert strat.data["rho"].isel(level=1).values == pytest.approx(-3.34617212e+08,rel=approx_rel)

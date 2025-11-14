import pytest
import os
import numpy as np
import xarray as xr
from aqua.diagnostics import Boxplots, PlotBoxplots
from conftest import APPROX_REL, DPI, LOGLEVEL

# Tolerance for numerical comparisons
approx_rel = APPROX_REL
loglevel = LOGLEVEL

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
# These tests use setup_class with shared resources (data fetching, tmp files)
pytestmark = [
    pytest.mark.diagnostics
]

# Common fixtures
@pytest.fixture(scope="module")
def boxplots_instance():
    """Create a Boxplots instance."""
    return Boxplots(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly')

@pytest.fixture(scope="module")
def plot_boxplots_instance():
    """Create a PlotBoxplots instance."""
    return PlotBoxplots(diagnostic='test', dpi=DPI)

@pytest.fixture
def tmp_path_str():
    """Provide consistent tmp_path as string."""
    return "./"


class TestBoxplots:
    """Test suite for Boxplots diagnostic."""
    
    def test_run_basic(self, boxplots_instance, plot_boxplots_instance, tmp_path_str):
        """Test basic boxplots run."""
        bp = boxplots_instance
        plotbp = plot_boxplots_instance
        var = ['tnlwrf', 'tnswrf']
        
        bp.run(var=var, save_netcdf=True)
        assert hasattr(bp, "fldmeans")
        assert isinstance(bp.fldmeans, xr.Dataset)
        assert all(v in bp.fldmeans for v in var)

        nc = os.path.join(tmp_path_str, 'netcdf', f'boxplots.boxplot.ci.ERA5.era5-hpz3.r1.tnlwrf_tnswrf.nc')
        assert os.path.exists(nc)

        plotbp.plot_boxplots(data=bp.fldmeans, data_ref=bp.fldmeans, var=var)

        pdf = os.path.join(tmp_path_str, 'pdf', f'test.boxplot.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.tnlwrf_tnswrf.pdf')
        assert os.path.exists(pdf)

        plotbp.plot_boxplots(data=bp.fldmeans, data_ref=bp.fldmeans, var=var, anomalies=True, add_mean_line=True)
        png = os.path.join(tmp_path_str, 'png', f'test.boxplot.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.tnlwrf_tnswrf.png')
        assert os.path.exists(png)

    def test_run_with_units(self, boxplots_instance):
        """Test boxplots run with custom units."""
        bp = boxplots_instance
        bp.run(var='tprate', units='mm/day', save_netcdf=True)
        assert bp.fldmeans['tprate'].attrs.get('units') == 'mm/day'

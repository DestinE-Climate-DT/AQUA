import pytest
import os
import numpy as np
import xarray as xr
from aqua.diagnostics import GlobalBiases, PlotGlobalBiases
from aqua.exceptions import NoDataError
from conftest import APPROX_REL, DPI, LOGLEVEL

# Tolerance for numerical comparisons
approx_rel = APPROX_REL
loglevel = LOGLEVEL

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
# These tests use setup_class with shared resources (data fetching, tmp files)
pytestmark = [
    pytest.mark.diagnostics
]

# Module-level fixtures
@pytest.fixture(scope="module")
def global_biases_instance():
    """Create a GlobalBiases instance with pre-fetched data."""
    gb = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly', regrid='r100')
    gb.retrieve()
    return gb

@pytest.fixture(scope="module")
def plot_global_biases_instance():
    """Create a PlotGlobalBiases instance."""
    return PlotGlobalBiases(dpi=DPI)

@pytest.fixture
def tmp_path_str():
    """Provide consistent tmp_path as string."""
    return "./"

@pytest.fixture
def test_var():
    """Variable to test with."""
    return 'q'

class TestGlobalBiases:
    """Test suite for GlobalBiases diagnostic."""
    
    def test_climatology(self, global_biases_instance, plot_global_biases_instance, 
                        tmp_path_str, test_var):

        gb = global_biases_instance
        plotgb = plot_global_biases_instance
        var = test_var
        
        gb.compute_climatology(var=var, seasonal=True)
        assert hasattr(gb, "climatology")
        assert hasattr(gb, "seasonal_climatology")
        assert isinstance(gb.climatology, xr.Dataset)
        assert isinstance(gb.seasonal_climatology, xr.Dataset)
        assert var in gb.climatology
        assert var in gb.seasonal_climatology
        assert "season" in gb.seasonal_climatology[var].dims
        assert set(gb.seasonal_climatology["season"].values) == {"DJF", "MAM", "JJA", "SON"}

        nc = os.path.join(tmp_path_str, 'netcdf', f'globalbiases.annual_climatology.ci.ERA5.era5-hpz3.r1.{var}.nc')
        assert os.path.exists(nc)

        nc_seasonal = os.path.join(tmp_path_str, 'netcdf', f'globalbiases.seasonal_climatology.ci.ERA5.era5-hpz3.r1.{var}.nc')
        assert os.path.exists(nc_seasonal)

        plotgb.plot_climatology(data=gb.climatology, var=var, plev=85000)

        pdf = os.path.join(tmp_path_str, 'pdf', f'globalbiases.annual_climatology.ci.ERA5.era5-hpz3.r1.{var}.85000.pdf')
        assert os.path.exists(pdf)

        png = os.path.join(tmp_path_str, 'png', f'globalbiases.annual_climatology.ci.ERA5.era5-hpz3.r1.{var}.85000.png')
        assert os.path.exists(png)

    def test_bias(self, global_biases_instance, plot_global_biases_instance, 
                  tmp_path_str, test_var):
        gb = global_biases_instance
        plotgb = plot_global_biases_instance
        var = test_var
        
        # Ensure climatology is computed (may be from previous test or run it)
        if not hasattr(gb, 'climatology'):
            gb.compute_climatology(var=var, seasonal=True)
        
        plotgb.plot_bias(data=gb.climatology, data_ref=gb.climatology, var=var, plev=85000)
        pdf = os.path.join(tmp_path_str, 'pdf', f'globalbiases.bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.85000.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(tmp_path_str, 'png', f'globalbiases.bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.85000.png')
        assert os.path.exists(png)

    def test_seasonal_bias(self, global_biases_instance, plot_global_biases_instance, 
                          tmp_path_str, test_var):
        gb = global_biases_instance
        plotgb = plot_global_biases_instance
        var = test_var
        
        # Ensure seasonal climatology is computed
        if not hasattr(gb, 'seasonal_climatology'):
            gb.compute_climatology(var=var, seasonal=True)
        
        plotgb.plot_seasonal_bias(data=gb.seasonal_climatology, data_ref=gb.seasonal_climatology, var=var, plev=85000)
        pdf = os.path.join(tmp_path_str, 'pdf', f'globalbiases.seasonal_bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.85000.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(tmp_path_str, 'png', f'globalbiases.seasonal_bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.85000.png')
        assert os.path.exists(png)

    def test_vertical_bias(self, global_biases_instance, plot_global_biases_instance, 
                          tmp_path_str, test_var):
        gb = global_biases_instance
        plotgb = plot_global_biases_instance
        var = test_var
        
        # Ensure climatology is computed
        if not hasattr(gb, 'climatology'):
            gb.compute_climatology(var=var, seasonal=True)
        
        plotgb.plot_vertical_bias(data=gb.climatology, data_ref=gb.climatology, var=var, vmin=-0.002, vmax=0.002)
        pdf = os.path.join(tmp_path_str, 'pdf', f'globalbiases.vertical_bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(tmp_path_str, 'png', f'globalbiases.vertical_bias.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.{var}.png')
        assert os.path.exists(png)

    def test_plev_selection(self, test_var):
        gb = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly', regrid='r100')
        
        gb.retrieve(var=test_var, plev=85000)
        gb.compute_climatology(var=test_var, plev=85000)
        assert gb.climatology['q'].coords['plev'] == 85000

        with pytest.raises(ValueError):
            gb.retrieve('tprate', plev=85000)

    def test_variables(self):
        gb_local = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly')
        with pytest.raises(Exception) as exc:
            gb_local.retrieve(var='pippo')
        assert isinstance(exc.value, (ValueError, NoDataError))

        gb_local.retrieve(var='tprate', units='mm/day')
        gb_local.compute_climatology(var='tprate')
        assert gb_local.climatology['tprate'].attrs.get('units') == 'mm/day'

    def test_formula(self):
        var = 'tnlwrf+tnswrf'
        long_name = 'Top net radiation'
        short_name = 'tnr'
        gb = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly')
        gb.retrieve(formula=True, var=var, long_name=long_name, short_name=short_name)
        gb.compute_climatology()
        assert short_name in gb.climatology.data_vars
        assert gb.data[short_name].attrs.get('long_name') == long_name
        assert gb.data[short_name].attrs.get('short_name') == short_name

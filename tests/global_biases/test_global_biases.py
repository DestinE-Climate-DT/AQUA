import pytest
import os
import numpy as np
import xarray as xr
from aqua.diagnostics import GlobalBiases, PlotGlobalBiases

# Tolerance for numerical comparisons
approx_rel = 1e-4
loglevel = 'DEBUG'

@pytest.mark.diagnostics
class TestGlobalBiases:
    @classmethod
    def setup_class(cls):
        cls.tmp_path = "./"
        cls.var = 'q'
        cls.gb = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly', regrid='r100')
        cls.plotgb = PlotGlobalBiases()
        cls.gb.retrieve()

    def test_climatology(self):
        self.gb.compute_climatology(var=self.var, seasonal=True)
        assert hasattr(self.gb, "climatology")
        assert hasattr(self.gb, "seasonal_climatology")
        assert isinstance(self.gb.climatology, xr.Dataset)
        assert isinstance(self.gb.seasonal_climatology, xr.Dataset)
        assert self.var in self.gb.climatology
        assert self.var in self.gb.seasonal_climatology
        assert "season" in self.gb.seasonal_climatology[self.var].dims
        assert set(self.gb.seasonal_climatology["season"].values) == {"DJF", "MAM", "JJA", "SON"}

        nc = os.path.join(self.tmp_path, 'netcdf', f'globalbiases.climatology.ci.ERA5.era5-hpz3.{self.var}.nc')
        assert os.path.exists(nc)

        nc_seasonal = os.path.join(self.tmp_path, 'netcdf', f'globalbiases.seasonal_climatology.ci.ERA5.era5-hpz3.{self.var}.nc')
        assert os.path.exists(nc_seasonal)

        self.plotgb.plot_climatology(data=self.gb.climatology, var=self.var, plev=85000)

        pdf = os.path.join(self.tmp_path, 'pdf', f'globalbiases.climatology.ci.ERA5.era5-hpz3.{self.var}.85000.pdf')
        assert os.path.exists(pdf)

        png = os.path.join(self.tmp_path, 'png', f'globalbiases.climatology.ci.ERA5.era5-hpz3.{self.var}.85000.png')
        assert os.path.exists(png)

    def test_bias(self):
        self.plotgb.plot_bias(data=self.gb.climatology, data_ref=self.gb.climatology, var=self.var, plev=85000)
        pdf = os.path.join(self.tmp_path, 'pdf', f'globalbiases.bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.85000.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(self.tmp_path, 'png', f'globalbiases.bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.85000.png')
        assert os.path.exists(png)

    def test_seasonal_bias(self):
        self.plotgb.plot_seasonal_bias(data=self.gb.seasonal_climatology, data_ref=self.gb.seasonal_climatology, var=self.var, plev=85000)
        pdf = os.path.join(self.tmp_path, 'pdf', f'globalbiases.seasonal_bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.85000.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(self.tmp_path, 'png', f'globalbiases.seasonal_bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.85000.png')
        assert os.path.exists(png)

    def test_vertical_bias(self):
        self.plotgb.plot_vertical_bias(data=self.gb.climatology, data_ref=self.gb.climatology, var=self.var, vmin=-0.002, vmax=0.002)
        pdf = os.path.join(self.tmp_path, 'pdf', f'globalbiases.vertical_bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.pdf')
        assert os.path.exists(pdf)
        png = os.path.join(self.tmp_path, 'png', f'globalbiases.vertical_bias.ci.ERA5.era5-hpz3.ERA5.era5-hpz3.{self.var}.png')
        assert os.path.exists(png)

    def test_plev_selection(self):
        self.gb.retrieve(var='q', plev=85000)
        self.gb.compute_climatology(var=self.var, plev=85000)
        assert self.gb.climatology['q'].coords['plev'] == 85000

        with pytest.raises(ValueError):
            self.gb.retrieve('tprate', plev=85000)

    def test_variables(self):
        gb_local = GlobalBiases(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly')
        with pytest.raises(ValueError):
            gb_local.retrieve(var='pippo')

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

import pytest
import os
import numpy as np
import xarray as xr
from aqua.diagnostics import Boxplots, PlotBoxplots

# Tolerance for numerical comparisons
approx_rel = 1e-4
loglevel = 'DEBUG'

@pytest.mark.diagnostics
class TestBoxplots:
    @classmethod
    def setup_class(cls):
        cls.tmp_path = "./"
        cls.var = ['tnlwrf', 'tnswrf']
        cls.bp = Boxplots(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly')
        cls.plotbp = PlotBoxplots(diagnostic='test')
    
    def test_run_basic(self):
        self.bp.run(var=self.var, save_netcdf=True)
        assert hasattr(self.bp, "fldmeans")
        assert isinstance(self.bp.fldmeans, xr.Dataset)
        assert all(v in self.bp.fldmeans for v in self.var)

        nc = os.path.join(self.tmp_path, 'netcdf', f'boxplots.boxplot.ci.ERA5.era5-hpz3.r1.tnlwrf_tnswrf.nc')
        assert os.path.exists(nc)

        self.plotbp.plot_boxplots(data=self.bp.fldmeans, data_ref=self.bp.fldmeans, var=self.var)

        pdf = os.path.join(self.tmp_path, 'pdf', f'test.boxplot.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.tnlwrf_tnswrf.pdf')
        assert os.path.exists(pdf)

        png = os.path.join(self.tmp_path, 'png', f'test.boxplot.ci.ERA5.era5-hpz3.r1.ERA5.era5-hpz3.tnlwrf_tnswrf.png')
        assert os.path.exists(png)

    def test_run_with_units(self):
        self.bp.run(var='tprate', units='mm/day', save_netcdf=True)
        assert self.bp.fldmeans['tprate'].attrs.get('units') == 'mm/day'
        

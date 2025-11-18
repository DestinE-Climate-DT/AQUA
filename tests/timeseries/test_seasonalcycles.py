import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import SeasonalCycles, PlotSeasonalCycles
from conftest import APPROX_REL, DPI, LOGLEVEL

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL


@pytest.mark.diagnostics
class TestSeasonalCycles:
    """Test the SeasonalCycles class."""

    def setup_method(self):
        """Initialize variables before each test."""
        self.diagnostic_name = 'atmosphere'
        self.catalog = 'ci'
        self.model = 'ERA5'
        self.exp = 'era5-hpz3'
        self.source = 'monthly'
        self.var = 'tcc'
        self.region = 'tropics'
        self.regrid = 'r100'
        self.startdate = '1990-01-01'
        self.enddate = '1991-12-31'

    def test_no_region(self, tmp_path):
        sc = SeasonalCycles(diagnostic_name=self.diagnostic_name,
                            catalog=self.catalog, model=self.model, exp=self.exp,
                            source=self.source, regrid=self.regrid,
                            startdate=self.startdate, enddate=self.enddate,
                            loglevel=loglevel)
        
        assert sc.lon_limits is None
        assert sc.lat_limits is None

        sc.run(var=self.var, outputdir=tmp_path, std=True)

        assert isinstance(sc.data, xr.DataArray)
        assert sc.monthly.values[0] == pytest.approx(63.22174285385192, rel=approx_rel)

        filename = f'{self.diagnostic_name}.seasonalcycles.{self.catalog}.{self.model}.{self.exp}.r1.{self.var}.monthly.global.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        assert sc.std_monthly.values[0] == pytest.approx(0.23421051986458963, rel=approx_rel)

        filename = f'{self.diagnostic_name}.seasonalcycles.{self.catalog}.{self.model}.{self.exp}.r1.{self.var}.monthly.global.std.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        plt = PlotSeasonalCycles(diagnostic_name=self.diagnostic_name,
                                 monthly_data = sc.monthly, ref_monthly_data = sc.monthly,
                                 std_monthly_data = sc.std_monthly, loglevel=loglevel)
        plt.run(outputdir=tmp_path, dpi=DPI)

        filename = f'{self.diagnostic_name}.seasonalcycles.{self.catalog}.{self.model}.{self.exp}.r1.{self.catalog}.{self.model}.{self.exp}.{self.var}.png'
        file = os.path.join(tmp_path, 'png', filename)
        assert os.path.exists(file)

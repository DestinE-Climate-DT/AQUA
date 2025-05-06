import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import Timeseries, PlotTimeseries

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.diagnostics
class TestTimeseries:
    """Test that the timeseries class works"""

    def setup_method(self):
        """Initialize variables before each test."""
        self.catalog = 'ci'
        self.model = 'ERA5'
        self.exp = 'era5-hpz3'
        self.source = 'monthly'
        self.var = 'tcc'
        self.region = 'tropics'
        self.regrid = 'r100'

    def test_no_region(self):
        ts = Timeseries(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=None, loglevel=loglevel, regrid=self.regrid)

        assert ts.lon_limits is None
        assert ts.lat_limits is None

    def test_wrong_region(self):
        with pytest.raises(ValueError):
            Timeseries(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                       region='topolinia', loglevel=loglevel, regrid=self.regrid)

    def test_monthly_annual_with_region(self, tmp_path):
        ts = Timeseries(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19900101', enddate='19911231',
                        regrid=self.regrid)
        
        ts.run(var=self.var, freq=['monthly', 'annual'], outputdir=tmp_path, std=True)

        assert ts.lon_limits == [-180, 180]
        assert ts.lat_limits == [-15, 15]

        assert isinstance(ts.data, xr.DataArray)
        assert ts.monthly.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

        file = os.path.join(tmp_path, 'netcdf', 'timeseries.tcc.monthly.tropics.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

        assert ts.annual.values[0] == pytest.approx(60.31101797654943, rel=approx_rel)

        assert ts.std_annual.values == pytest.approx(0.009666691494246038, rel=approx_rel)

        file = os.path.join(tmp_path, 'netcdf', 'timeseries.tcc.annual.tropics.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

        file = os.path.join(tmp_path, 'netcdf', 'timeseries.tcc.annual.tropics.std.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

        plt = PlotTimeseries(monthly_data = ts.monthly, annual_data = ts.annual,
                             ref_monthly_data = ts.monthly, ref_annual_data = ts.annual,
                             std_monthly_data = ts.std_monthly, std_annual_data = ts.std_annual,
                             loglevel=loglevel)
        
        plt.run(var=self.var, outputdir=tmp_path)

        file = os.path.join(tmp_path, 'png', 'timeseries.timeseries.ci.ERA5.era5-hpz3.tcc.png')
        assert os.path.exists(file)

    def test_hourly_daily_with_region(self):
        ts = Timeseries(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19900101', enddate='19900102',
                        regrid=self.regrid)

        ts.retrieve(var=self.var)

        ts.compute(freq='hourly')
        assert ts.hourly.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

        ts.compute(freq='daily')
        assert ts.daily.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

    def test_formula(self):
        ts = Timeseries(catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19940101', enddate='19950101',
                        regrid=self.regrid)

        ts.retrieve(var='2*tcc', formula=True, standard_name='2tcc', long_name='2*Total Cloud Cover', units='%')

        ts.compute(freq='monthly')
        assert ts.monthly.values[0] ==  pytest.approx(117.40372092960037, rel=approx_rel)
        # The extra month added should be the same as the first one since there is only one year
        assert ts.monthly.values[-1] == pytest.approx(117.40372092960037, rel=approx_rel)

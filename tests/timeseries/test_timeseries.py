import os
import pytest
import xarray as xr
from aqua import Reader
from aqua.diagnostics.timeseries import Timeseries, PlotTimeseries
from conftest import APPROX_REL, DPI, LOGLEVEL

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL


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
        self.diagnostic_name = 'atmosphere'

    def test_no_region(self):
        ts = Timeseries(diagnostic_name=self.diagnostic_name, 
                        catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=None, loglevel=loglevel, regrid=self.regrid)

        assert ts.lon_limits is None
        assert ts.lat_limits is None

    def test_wrong_region(self):
        with pytest.raises(ValueError):
            Timeseries(diagnostic_name=self.diagnostic_name, 
                       catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                       region='topolinia', loglevel=loglevel, regrid=self.regrid)

    def test_all_freq_with_region(self, tmp_path):
        ts = Timeseries(diagnostic_name=self.diagnostic_name,
                        catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19900101', enddate='19911231',
                        regrid=self.regrid)

        ts.run(var=self.var, freq=['monthly', 'annual'], outputdir=tmp_path, std=True, create_catalog_entry=True)

        assert ts.lon_limits == [-180, 180]
        assert ts.lat_limits == [-15, 15]

        reader = Reader(catalog=self.catalog, model=self.model, exp=self.exp,
                        source=f'aqua-{self.diagnostic_name}-timeseries',
                        freq='monthly', loglevel=loglevel, areas=False)
        data = reader.retrieve()[self.var]
        assert isinstance(ts.data, xr.DataArray)
        assert data.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

        filename = f'{self.diagnostic_name}.timeseries.{self.catalog}.{self.model}.{self.exp}.r1.{self.var}.monthly.{self.region}.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        assert ts.annual.values[0] == pytest.approx(60.31101797654943, rel=approx_rel)
        
        assert ts.std_annual.values == pytest.approx(0.009666691494246038, rel=approx_rel)

        filename = f'{self.diagnostic_name}.timeseries.{self.catalog}.{self.model}.{self.exp}.r1.{self.var}.annual.{self.region}.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        filename = f'{self.diagnostic_name}.timeseries.{self.catalog}.{self.model}.{self.exp}.r1.{self.var}.monthly.{self.region}.std.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        plt = PlotTimeseries(diagnostic_name=self.diagnostic_name,
                             monthly_data = ts.monthly, annual_data = ts.annual,
                             ref_monthly_data = ts.monthly, ref_annual_data = ts.annual,
                             std_monthly_data = ts.std_monthly, std_annual_data = ts.std_annual,
                             loglevel=loglevel)
        
        plt.run(outputdir=tmp_path, dpi=DPI)

        filename = f'{self.diagnostic_name}.timeseries.{self.catalog}.{self.model}.{self.exp}.r1.{self.catalog}.{self.model}.{self.exp}.{self.var}.{self.region}.png'
        file = os.path.join(tmp_path, 'png', filename)
        assert os.path.exists(file)

    def test_hourly_daily_with_region(self):
        ts = Timeseries(diagnostic_name=self.diagnostic_name,
                        catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19900101', enddate='19900301',
                        regrid=self.regrid)

        ts.retrieve(var=self.var)

        ts.compute(freq='hourly')
        assert ts.hourly.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

        ts.compute(freq='daily')
        assert ts.daily.values[0] == pytest.approx(60.145472982004186, rel=approx_rel)

    def test_formula(self):
        ts = Timeseries(diagnostic_name=self.diagnostic_name,
                        catalog=self.catalog, model=self.model, exp=self.exp, source=self.source,
                        region=self.region, loglevel=loglevel, startdate='19940101', enddate='19941231',
                        regrid=self.regrid)

        ts.retrieve(var='2*tcc', formula=True, short_name='2tcc', long_name='2*Total Cloud Cover', units='%')

        ts.compute(freq='monthly')
        assert ts.monthly.values[0] ==  pytest.approx(117.40372092960037, rel=approx_rel)
        # Differently from the previous version of the test, there is no extension of the data
        assert ts.monthly.values[-1] == pytest.approx(123.01323353753897, rel=approx_rel)

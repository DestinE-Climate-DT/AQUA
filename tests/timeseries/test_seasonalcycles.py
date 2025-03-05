import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import SeasonalCycles

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.diagnostics
class TestSeasonalCycles:
    """Test the SeasonalCycles class."""

    def setup_method(self):
        """Initialize variables before each test."""
        self.catalog = 'ci'
        self.model = 'ERA5'
        self.exp = 'era5-hpz3'
        self.source = 'monthly'
        self.var = 'tcc'
        self.region = 'tropics'
        self.regrid = 'r100'

    def test_no_region(self, tmp_path):
        sc = SeasonalCycles(catalog=self.catalog, model=self.model, exp=self.exp,
                            source=self.source, regrid=self.regrid, loglevel=loglevel)
        
        assert sc.lon_limits  is None

        sc.run(var=self.var, outputdir=tmp_path, std=True)

        assert isinstance(sc.data, xr.DataArray)
        assert sc.monthly.values[0] == pytest.approx(62.780865381833294, rel=approx_rel)

        file = os.path.join(tmp_path, 'netcdf', 'seasonalcycles.tcc.monthly.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

        assert sc.std_monthly.values[0] == pytest.approx(0.6491141598788776, rel=approx_rel)

        file = os.path.join(tmp_path, 'netcdf', 'seasonalcycles.tcc.monthly.std.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

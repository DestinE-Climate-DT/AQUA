import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import Gregory, PlotGregory
from conftest import APPROX_REL, DPI, LOGLEVEL

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL

@pytest.mark.diagnostics
class TestGregory:
    """Test the Gregory class."""

    def setup_method(self):
        """Initialize the variables before each test."""
        self.catalog = 'ci'
        self.model = 'ERA5'
        self.exp = 'era5-hpz3'
        self.source = 'monthly'
        self.regrid = 'r100'
        self.std_startdate = '1990-01-01'
        self.std_enddate = '1991-12-31'
        self.diagnostic_name = 'radiation'

    def test_gregory(self, tmp_path):
        """Test the Gregory class."""
        gp = Gregory(diagnostic_name=self.diagnostic_name,
                     catalog=self.catalog,
                     model=self.model,
                     exp=self.exp,
                     source=self.source,
                     regrid=self.regrid,
                     startdate=self.std_startdate,
                     enddate=self.std_enddate,
                     loglevel=loglevel)
        
        gp.run(std=True, outputdir=tmp_path)

        assert isinstance(gp.t2m, xr.DataArray)
        assert isinstance(gp.net_toa, xr.DataArray)

        assert gp.t2m_monthly.values[0] == pytest.approx(12.274455935718379, rel=approx_rel)
        assert gp.net_toa_monthly.values[0] == pytest.approx(7.86250579018185, rel=approx_rel)

        assert gp.t2m_std.values == pytest.approx(0.0277312, rel=approx_rel)
        assert gp.net_toa_std.values == pytest.approx(0.52176817, rel=approx_rel)

        filename = f'{self.diagnostic_name}.gregory.{self.catalog}.{self.model}.{self.exp}.r1.2t.annual.nc'
        file = os.path.join(tmp_path, 'netcdf', filename)
        assert os.path.exists(file)

        plt = PlotGregory(diagnostic_name=self.diagnostic_name,
                          t2m_monthly_data = gp.t2m_monthly,
                          net_toa_monthly_data = gp.net_toa_monthly,
                          t2m_annual_data = gp.t2m_annual,
                          net_toa_annual_data = gp.net_toa_annual,
                          t2m_monthly_ref = gp.t2m_monthly,
                          net_toa_monthly_ref = gp.net_toa_monthly,
                          t2m_annual_ref = gp.t2m_annual,
                          net_toa_annual_ref = gp.net_toa_annual,
                          t2m_annual_std = gp.t2m_std,
                          net_toa_annual_std = gp.net_toa_std,
                          loglevel=loglevel)
        
        title = plt.set_title()
        data_labels = plt.set_data_labels()
        ref_label = plt.set_ref_label()
        fig = plt.plot(title=title, data_labels=data_labels, ref_label=ref_label)
        _ = plt.set_description()
        plt.save_plot(fig, outputdir=tmp_path, diagnostic_product='gregory', dpi=DPI)

        filename = f'{self.diagnostic_name}.gregory.{self.catalog}.{self.model}.{self.exp}.r1.multiref.png'
        file = os.path.join(tmp_path, 'png', filename)
        assert os.path.exists(file)


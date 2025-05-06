import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import Gregory, PlotGregory

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

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

    def test_gregory(self, tmp_path):
        """Test the Gregory class."""
        gp = Gregory(catalog=self.catalog,
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

        file = os.path.join(tmp_path, 'netcdf', 'gregory.2t.annual.ci.ERA5.era5-hpz3.nc')
        assert os.path.exists(file)

        plt = PlotGregory(t2m_monthly_data = gp.t2m_monthly,
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
        description = plt.set_description()
        plt.save_plot(fig, description=description, outputdir=tmp_path, diagnostic='gregory')

        file = os.path.join(tmp_path, 'png', 'timeseries.gregory.ci.ERA5.era5-hpz3.png')
        assert os.path.exists(file)


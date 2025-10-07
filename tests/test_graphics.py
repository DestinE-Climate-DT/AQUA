import pytest
import os
import cartopy.crs as ccrs
from aqua import Reader
from aqua.graphics import plot_single_map, plot_single_map_diff
from aqua.graphics import plot_vertical_profile, plot_vertical_profile_diff
from aqua.graphics import plot_timeseries, plot_seasonalcycle
from aqua.graphics import plot_maps, plot_maps_diff, plot_hovmoller
from aqua.graphics import plot_vertical_lines

loglevel = "DEBUG"


@pytest.mark.graphics
class TestMaps:
    """Basic tests for the Single map functions"""
    def setup_method(self):
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d",
                        regrid="r200", fix=False, loglevel=loglevel)
        self.data = reader.retrieve(var='sst')

    def test_plot_single_map(self, tmp_path):
        """
        Test the plot_single_map function
        """
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        fig, ax = plot_single_map(data=plot_data,
                                  proj=ccrs.PlateCarree(),
                                  contour=False,
                                  extent=[-180, 180, -90, 90],
                                  nlevels=5,
                                  vmin=-2.0,
                                  vmax=30.0,
                                  sym=True,
                                  cmap='viridis',
                                  display=False,
                                  return_fig=True,
                                  transform_first=False,
                                  title='Test plot',
                                  cbar_label='Sea surface temperature [°C]',
                                  dpi=100,
                                  nxticks=5,
                                  nyticks=5,
                                  ticks_rounding=1,
                                  cbar_ticks_rounding=1,
                                  loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_single_map.png')
        assert os.path.exists(tmp_path / 'test_plot_single_map.png')

    def test_plot_single_map_diff(self, tmp_path):
        """
        Test the plot_single_map_diff function
        """
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        plot_data2 = self.data["sst"].isel(time=1).aqua.regrid()
        fig, ax = plot_single_map_diff(data=plot_data,
                                       data_ref=plot_data2,
                                       nlevels=5,
                                       vmin_fill=-5.0,
                                       vmax_fill=5.0,
                                       sym=False,
                                       vmin_contour=-2.0,
                                       vmax_contour=30.0,
                                       sym_contour=True,
                                       cmap='viridis',
                                       display=False,
                                       return_fig=True,
                                       title='Test plot',
                                       cbar_label='Sea surface temperature [°C]',
                                       dpi=100,
                                       nxticks=5,
                                       nyticks=5,
                                       gridlines=True,
                                       loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_single_map_diff.png')
        assert os.path.exists(tmp_path / 'test_plot_single_map_diff.png')

    def test_plot_single_map_no_diff(self):
        """
        Test the plot_single_map_diff function
        """
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        plot_data2 = plot_data.copy()

        fig, ax = plot_single_map_diff(data=plot_data, return_fig=True,
                                       data_ref=plot_data2, loglevel=loglevel)

        assert fig is not None
        assert ax is not None

    def test_maps(self, tmp_path):
        """Test plot_maps function"""
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        plot_data2 = self.data["sst"].isel(time=1).aqua.regrid()
        fig = plot_maps(maps=[plot_data, plot_data2],
                        nlevels=5,
                        vmin=-2, vmax=30,
                        sym=False,
                        cmap='viridis',
                        title='Test plot',
                        titles=['Test plot 1', 'Test plot 2'],
                        cbar_label='Sea surface temperature [°C]',
                        nxticks=5, nyticks=6,
                        return_fig=True, loglevel=loglevel)
        assert fig is not None

        fig.savefig(tmp_path / 'test_plot_maps.png')
        assert os.path.exists(tmp_path / 'test_plot_maps.png')

        fig2 = plot_maps_diff(maps=[plot_data, plot_data],
                              maps_ref=[plot_data2, plot_data2],
                              nlevels=5,
                              vmin_fill=-2, vmax_fill=2,
                              vmin_contour=-2, vmax_contour=30,
                              sym=False, sym_contour=True,
                              cmap='viridis',
                              title='Test plot',
                              titles=['Test plot 1', 'Test plot 2'],
                              cbar_label='Sea surface temperature [°C]',
                              nxticks=5, nyticks=6,
                              return_fig=True, loglevel=loglevel)

        assert fig2 is not None

        fig2.savefig(tmp_path / 'test_plot_maps_diff.png')
        assert os.path.exists(tmp_path / 'test_plot_maps_diff.png')

    def test_maps_error(self):
        """Test plot_maps function with error"""
        with pytest.raises(ValueError):
            plot_maps(maps="test")

        with pytest.raises(ValueError):
            plot_maps_diff(maps="test", maps_ref="test")


@pytest.mark.graphics
class TestVerticalProfiles:
    """Basic tests for the Vertical Profile functions"""

    def setup_method(self):
        """Setup method to retrieve data for testing"""
        model = 'ERA5'
        exp = 'era5-hpz3'
        source = 'monthly'
        self.reader = Reader(model=model, exp=exp, source=source, regrid='r100')
        self.data = self.reader.retrieve(['q'])
        self.data = self.reader.regrid(self.data)


    def test_plot_vertical_profile(self, tmp_path):
        """Test the plot_vertical_profile function"""
        fig, ax = plot_vertical_profile(data=self.data['q'].isel(time=0).mean('lon'),
                                        var='q',
                                        vmin=-0.002,
                                        vmax=0.002,
                                        nlevels=8,
                                        return_fig=True,
                                        loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_vertical_profile.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_vertical_profile.png')

    def test_plot_vertical_profile_diff(self, tmp_path):
        """Test the plot_vertical_profile_diff function"""
        fig, ax = plot_vertical_profile_diff(data=self.data['q'].isel(time=0).mean('lon'),
                                            data_ref=self.data['q'].isel(time=1).mean('lon'),
                                            var='q',
                                            vmin=-0.002,
                                            vmax=0.002,
                                            vmin_contour=-0.002,
                                            vmax_contour=0.002,
                                            add_contour=True,
                                            nlevels=8,
                                            return_fig=True,
                                            loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_vertical_profile_diff.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_vertical_profile_diff.png')
        
@pytest.mark.graphics
class TestTimeseries:
    """Basic tests for the Timeseries functions"""

    def setup_method(self):
        """Setup method to retrieve data for testing"""
        model = 'IFS'
        exp = 'test-tco79'
        source = 'teleconnections'
        var = 'skt'
        self.reader = Reader(model=model, exp=exp, source=source, fix=True)
        data = self.reader.retrieve(var=var)

        self.t1 = data[var].isel(lat=1, lon=1)
        self.t2 = data[var].isel(lat=10, lon=10)

    def test_plot_timeseries(self, tmp_path):
        """Test the plot_timeseries function"""
        t1_yearly = self.reader.timmean(self.t1, freq='YS', center_time=True)
        t2_yearly = self.reader.timmean(self.t2, freq='YS', center_time=True)
        std_mon = self.t1.groupby('time.month').std('time')
        std_annual = t1_yearly.std(dim='time')

        data_labels = ['t1', 't2']

        fig, ax = plot_timeseries(monthly_data=[self.t1, self.t2], annual_data=[t1_yearly, t2_yearly],
                                  ref_monthly_data=self.t1,
                                  ref_annual_data=t1_yearly,
                                  std_monthly_data=std_mon,
                                  std_annual_data=std_annual,
                                  ref_label=data_labels[0],
                                  title='Temperature at two locations',
                                  data_labels=data_labels)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_timeseries.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_timeseries.png')

    def test_plot_seasonalcycle(self, tmp_path):
        """Test the plot_seasonalcycle function"""
        t1_seasonal = self.t1.groupby('time.month').mean('time')
        t2_seasonal = self.t2.groupby('time.month').mean('time')
        std_data = self.t1.groupby('time.month').std('time')

        fig, ax = plot_seasonalcycle(data=t1_seasonal, ref_data=t2_seasonal,
                                     std_data=std_data,
                                     title='Seasonal cycle of temperature at two locations',
                                     data_labels='t1',
                                     ref_label='t2',
                                     loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_seasonalcycle.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_seasonalcycle.png')

    def test_plot_ensemble(self, tmp_path):
        """Test the plot_timeseries function with ensemble data"""
        t1_yearly = self.reader.timmean(self.t1, freq='YS', center_time=True)
        t2_yearly = self.reader.timmean(self.t2, freq='YS', center_time=True)

        # Create ensemble mean and standard deviation (fake data for testing)
        ens_mon_mean = (t1_yearly + t2_yearly) / 2
        
        # simply using the mean here, the function will plot: mean +/- 2xSTD
        # NOTE: the STD is pointwise along time axis
        #ens_mon_std = ens_mon_mean.groupby('time.month').std(dim='time') 
        ens_mon_std = ens_mon_mean 
        ens_annual_mean = self.reader.timmean(ens_mon_mean, freq='YS', center_time=True)

        # NOTE: Similarly, we will use annual mean as STD for testing purposes
        # as done in the previous lines
        #ens_annual_std = ens_annual_mean.std(dim='time')
        ens_annual_std = ens_annual_mean.std(dim='time')

        fig, ax = plot_timeseries(monthly_data=[self.t1, self.t2],
                                  annual_data=[t1_yearly, t2_yearly],
                                  ens_monthly_data=ens_mon_mean,
                                  std_ens_monthly_data=ens_mon_std,
                                  ens_annual_data=ens_annual_mean,
                                  std_ens_annual_data=ens_annual_std,
                                  ens_label='Ensemble mean',
                                  title='Ensemble mean temperature at two locations')

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_ensemble.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_ensemble.png')


@pytest.mark.graphics
class TestHovmoller:
    """Basic tests for the Hovmoller functions"""

    def setup_method(self):
        model = 'IFS'
        exp = 'test-tco79'
        source = 'teleconnections'
        var = 'skt'
        self.reader = Reader(model=model, exp=exp, source=source, fix=False)
        data = self.reader.retrieve(var=var)

        self.data = data[var]

    def test_plot_hovmoller(self, tmp_path):
        """Test the plot_hovmoller function"""
        fig2, ax2 = plot_hovmoller(data=self.data,
                                   return_fig=True,
                                   cmap='RdBu_r',
                                   invert_axis=True,
                                   invert_time=True,
                                   cbar_label='test-label',
                                   nlevels=10,
                                   sym=True,
                                   loglevel=loglevel)

        assert fig2 is not None
        assert ax2 is not None

        fig2.savefig(tmp_path / 'test_hovmoller2.png')
        assert os.path.exists(tmp_path / 'test_hovmoller2.png')

        fig, _ = plot_hovmoller(data=self.data,
                                 return_fig=True,
                                 contour=False,
                                 cmap='RdBu_r',
                                 invert_time=True,
                                 loglevel=loglevel)

        assert fig is not None

        fig.savefig(tmp_path / 'test_hovmoller3.png')

        assert os.path.exists(tmp_path / 'test_hovmoller3.png')

    def test_plot_hovmoller_error(self):

        with pytest.raises(TypeError):
            plot_hovmoller(data='test')


@pytest.mark.graphics
class TestVerticalLines:
    """Basic tests for the Vertical Line functions"""

    def setup_method(self):
        """Setup method to retrieve data for testing"""
        model = 'ERA5'
        exp = 'era5-hpz3'
        source = 'monthly'
        self.reader = Reader(model=model, exp=exp, source=source)
        self.data = self.reader.retrieve(['q'])['q'].isel(time=0, cells=0)

    def test_plot_vertical_lines(self, tmp_path):
        """Test the plot_vertical_lines function"""
        fig, ax = plot_vertical_lines(data=self.data,
                                      ref_data=self.data * 0.8,
                                      lev_name='plev',
                                      labels=['test'],
                                      ref_label='ref',
                                      title='Test vertical line',
                                      return_fig=True,
                                      invert_yaxis=True,
                                      loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_vertical_lines.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_vertical_lines.png')
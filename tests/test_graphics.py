import pytest
import os
import numpy as np
import xarray as xr
import matplotlib.pyplot as plt
import cartopy.crs as ccrs
from aqua import Reader
from aqua.core.graphics import plot_single_map, plot_single_map_diff
from aqua.core.graphics import plot_vertical_profile, plot_vertical_profile_diff
from aqua.core.graphics import plot_timeseries, plot_seasonalcycle
from aqua.core.graphics import plot_maps, plot_maps_diff, plot_hovmoller
from aqua.core.graphics import plot_vertical_lines, plot_histogram
from aqua.core.graphics import plot_lat_lon_profiles, plot_seasonal_lat_lon_profiles
from conftest import DPI, LOGLEVEL

loglevel = LOGLEVEL

# Aliases with module scope for fixtures
@pytest.fixture(scope='module')
def reader_era5(era5_hpz3_monthly_reader):
    return era5_hpz3_monthly_reader

@pytest.fixture(scope='module')
def data_era5(era5_hpz3_monthly_data):
    return era5_hpz3_monthly_data

@pytest.fixture(scope='module')
def reader_era5_r100(era5_hpz3_monthly_r100_reader):
    return era5_hpz3_monthly_r100_reader

@pytest.fixture(scope='module')
def data_era5_r100(era5_hpz3_monthly_r100_data):
    return era5_hpz3_monthly_r100_data

@pytest.fixture(scope='module')
def fesom_r200_fixFalse_reader(fesom_test_pi_original_2d_r200_fixFalse_reader):
    return fesom_test_pi_original_2d_r200_fixFalse_reader

@pytest.fixture(scope='module')
def fesom_r200_fixFalse_data(fesom_test_pi_original_2d_r200_fixFalse_data):
    return fesom_test_pi_original_2d_r200_fixFalse_data

@pytest.fixture(scope='module')
def reader_ifs_tc():
    return Reader(model='IFS', exp='test-tco79', source='teleconnections', fix=True)

@pytest.fixture(scope='module')
def data_ifs_tc(reader_ifs_tc):
    return reader_ifs_tc.retrieve(var='skt')

@pytest.mark.graphics
class TestMaps:
    """Basic tests for the Single map functions"""

    def test_plot_single_map(self, tmp_path, fesom_r200_fixFalse_reader, fesom_r200_fixFalse_data):
        """
        Test the plot_single_map function
        """
        data_regrid = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data = data_regrid["sst"].isel(time=0)
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
                                  dpi=DPI,
                                  nxticks=5,
                                  nyticks=5,
                                  ticks_rounding=1,
                                  cbar_ticks_rounding=1,
                                  loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_single_map.png')
        assert os.path.exists(tmp_path / 'test_plot_single_map.png')

    def test_plot_single_map_diff(self, tmp_path, fesom_r200_fixFalse_reader, fesom_r200_fixFalse_data):
        """
        Test the plot_single_map_diff function
        """
        data_regrid = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data = data_regrid["sst"].isel(time=0)
        data_regrid2 = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data2 = data_regrid2["sst"].isel(time=1)

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
                                       dpi=DPI,
                                       nxticks=5,
                                       nyticks=5,
                                       gridlines=True,
                                       loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_single_map_diff.png')
        assert os.path.exists(tmp_path / 'test_plot_single_map_diff.png')

    def test_plot_single_map_no_diff(self, fesom_r200_fixFalse_reader, fesom_r200_fixFalse_data):
        """
        Test the plot_single_map_diff function
        """
        data_regrid = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data = data_regrid["sst"].isel(time=0)
        plot_data2 = plot_data.copy()

        fig, ax = plot_single_map_diff(data=plot_data, return_fig=True,
                                       data_ref=plot_data2, loglevel=loglevel)

        assert fig is not None
        assert ax is not None

    def test_maps(self, tmp_path, fesom_r200_fixFalse_reader, fesom_r200_fixFalse_data):
        """Test plot_maps function"""
        data_regrid = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data = data_regrid["sst"].isel(time=0)
        data_regrid2 = fesom_r200_fixFalse_reader.regrid(fesom_r200_fixFalse_data)
        plot_data2 = data_regrid2["sst"].isel(time=1)
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

        fig.savefig(tmp_path / 'test_plot_maps.png', dpi=DPI)
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

        fig2.savefig(tmp_path / 'test_plot_maps_diff.png', dpi=DPI)
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

    @pytest.fixture(autouse=True)
    def setup(self, reader_era5_r100, data_era5_r100):
        self.reader = reader_era5_r100
        self.data = data_era5_r100
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

        fig.savefig(tmp_path / 'test_plot_vertical_profile.png', dpi=DPI)

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

        fig.savefig(tmp_path / 'test_plot_vertical_profile_diff.png', dpi=DPI)

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

        fig.savefig(tmp_path / 'test_plot_timeseries.png', dpi=DPI)

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

        fig.savefig(tmp_path / 'test_seasonalcycle.png', dpi=DPI)

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

        fig.savefig(tmp_path / 'test_plot_ensemble.png', dpi=DPI)

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

        fig2.savefig(tmp_path / 'test_hovmoller2.png', dpi=DPI)
        assert os.path.exists(tmp_path / 'test_hovmoller2.png')

        fig, _ = plot_hovmoller(data=self.data,
                                 return_fig=True,
                                 contour=False,
                                 cmap='RdBu_r',
                                 invert_time=True,
                                 loglevel=loglevel)

        assert fig is not None

        fig.savefig(tmp_path / 'test_hovmoller3.png', dpi=DPI)

        assert os.path.exists(tmp_path / 'test_hovmoller3.png')

    def test_plot_hovmoller_error(self):

        with pytest.raises(TypeError):
            plot_hovmoller(data='test')


@pytest.mark.graphics
class TestVerticalLines:
    """Basic tests for the Vertical Line functions"""

    @pytest.fixture(autouse=True)
    def setup(self, reader_era5, data_era5):
        self.reader = reader_era5
        self.data = data_era5['q'].isel(time=0, cells=0)
        
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

        fig.savefig(tmp_path / 'test_plot_vertical_lines.png', dpi=DPI)

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_plot_vertical_lines.png')

@pytest.mark.graphics
class TestLatLonProfiles:
    """Basic tests for the lat_lon_profiles function"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        """Setup method to retrieve data for testing"""
        data = data_ifs_tc
        
        self.lat_profile = data['skt'].mean(dim='lon')
        self.lon_profile = data['skt'].mean(dim='lat')

    def _save_and_check(self, fig, tmp_path, filename):
        """Helper to save figure and verify file exists"""
        filepath = tmp_path / filename
        fig.savefig(filepath)
        assert os.path.exists(filepath)

    def test_plot_lat_lon_profiles_single(self, tmp_path):
        """Test plot_lat_lon_profiles with single DataArray"""
        fig, ax = plot_lat_lon_profiles(data=self.lat_profile.isel(time=0),
                                        title='Latitude profile test',
                                        data_labels=['Test data'],
                                        loglevel=loglevel)

        assert fig is not None
        assert ax is not None
        self._save_and_check(fig, tmp_path, 'test_lat_profile.png')

    def test_plot_lat_lon_profiles_multiple(self, tmp_path):
        """Test plot_lat_lon_profiles with multiple DataArrays"""
        data_list = [self.lat_profile.isel(time=0), 
                     self.lat_profile.isel(time=1)]
        
        fig, ax = plot_lat_lon_profiles(data=data_list,
                                        data_labels=['Time 0', 'Time 1'],
                                        title='Multiple latitude profiles',
                                        loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        self._save_and_check(fig, tmp_path, 'test_lat_profiles_multiple.png')

    def test_plot_lat_lon_profiles_with_ref(self, tmp_path):
        """Test plot_lat_lon_profiles with reference data"""
        data = self.lat_profile.isel(time=0)
        ref = self.lat_profile.isel(time=1)
        ref_std = self.lat_profile.std(dim='time')
        
        fig, ax = plot_lat_lon_profiles(data=data,
                                        ref_data=ref,
                                        ref_std_data=ref_std,
                                        data_labels=['Data'],
                                        ref_label='Reference',
                                        title='Profile with reference',
                                        loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        self._save_and_check(fig, tmp_path, 'test_lat_profile_with_ref.png')

    def test_plot_lon_profile(self, tmp_path):
        """Test plot_lat_lon_profiles with longitude profile"""
        fig, ax = plot_lat_lon_profiles(data=self.lon_profile.isel(time=0),
                                        title='Longitude profile test',
                                        loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        self._save_and_check(fig, tmp_path, 'test_lon_profile.png')

    def test_plot_lat_lon_profiles_no_spatial_coords(self, tmp_path):
        """Test plot_lat_lon_profiles with DataArray without spatial coordinates"""
        data_no_coords = xr.DataArray(
            np.random.rand(10),
            dims=['time'],
            coords={'time': range(10)}
        )

        fig, ax = plot_lat_lon_profiles(data=data_no_coords,
                                        title='No spatial coordinates test',
                                        loglevel=loglevel)

        assert fig is not None
        assert ax is not None
        assert len(ax.lines) == 0  # No lines should be plotted
        self._save_and_check(fig, tmp_path, 'test_lat_profile_no_coords.png')

@pytest.mark.graphics
class TestSeasonalMeans:
    """Basic tests for the Seasonal Means functions"""

    @pytest.fixture(autouse=True)
    def setup(self, data_ifs_tc):
        """Setup method to retrieve data for testing"""
        data = data_ifs_tc
        
        self.data_seasonal = data['skt'].mean(dim='lon')
        
        # Create seasonal data for DJF, MAM, JJA, SON
        # Using simple time slicing for testing purposes
        self.djf = self.data_seasonal.isel(time=slice(0, 3)).mean(dim='time')
        self.mam = self.data_seasonal.isel(time=slice(3, 6)).mean(dim='time')
        self.jja = self.data_seasonal.isel(time=slice(6, 9)).mean(dim='time')
        self.son = self.data_seasonal.isel(time=slice(9, 12)).mean(dim='time')

    def test_plot_seasonal_lat_lon_profiles(self, tmp_path):
        """Test plot_seasonal_lat_lon_profiles function"""
        
        seasonal_data = [self.djf, self.mam, self.jja, self.son]
        
        fig, axs = plot_seasonal_lat_lon_profiles(seasonal_data=seasonal_data,
                                                  title='Seasonal Profiles Test',
                                                  data_labels=['Test data'],
                                                  loglevel=loglevel)
        
        assert fig is not None
        assert axs is not None
        assert len(axs) == 4
        

        fig.savefig(tmp_path / 'test_seasonal_profiles.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_seasonal_profiles.png')

    def test_plot_seasonal_lat_lon_profiles_with_ref(self, tmp_path):
        """Test plot_seasonal_lat_lon_profiles with reference data"""
        
        seasonal_data = [self.djf, self.mam, self.jja, self.son]
        # Use slightly modified data as reference
        ref_data = [self.djf * 0.95, self.mam * 0.95, 
                    self.jja * 0.95, self.son * 0.95]
        ref_std = [self.djf.std(), self.mam.std(), 
                   self.jja.std(), self.son.std()]
        
        fig, axs = plot_seasonal_lat_lon_profiles(seasonal_data=seasonal_data,
                                                  ref_data=ref_data,
                                                  ref_std_data=ref_std,
                                                  title='Seasonal Profiles with Reference',
                                                  data_labels=['Data'],
                                                  ref_label='Reference',
                                                  loglevel=loglevel)
        
        assert fig is not None
        assert axs is not None
        
        fig.savefig(tmp_path / 'test_seasonal_profiles_with_ref.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_seasonal_profiles_with_ref.png')

    def test_plot_seasonal_lat_lon_profiles_multiple(self, tmp_path):
        """Test plot_seasonal_lat_lon_profiles with multiple models"""
        
        # Create second set of data (slightly different)
        djf2 = self.djf * 1.05
        mam2 = self.mam * 1.05
        jja2 = self.jja * 1.05
        son2 = self.son * 1.05
        
        seasonal_data = [[self.djf, djf2], [self.mam, mam2], 
                        [self.jja, jja2], [self.son, son2]]
        
        fig, axs = plot_seasonal_lat_lon_profiles(seasonal_data=seasonal_data,
                                                  title='Multiple Models Seasonal Profiles',
                                                  data_labels=['Model 1', 'Model 2'],
                                                  loglevel=loglevel)
        
        assert fig is not None
        assert axs is not None
        
        fig.savefig(tmp_path / 'test_seasonal_profiles_multiple.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_seasonal_profiles_multiple.png')

    def test_plot_seasonal_lat_lon_profiles_error(self):
        """Test plot_seasonal_lat_lon_profiles with invalid input"""
        
        # Test with wrong number of seasons
        with pytest.raises(ValueError):
            plot_seasonal_lat_lon_profiles(seasonal_data=[self.djf, self.mam])
        
        # Test with non-list input
        with pytest.raises(ValueError):
            plot_seasonal_lat_lon_profiles(seasonal_data=self.djf)

    def test_plot_seasonal_lat_lon_profiles_with_none_ref_std(self, tmp_path):
        """Test plot_seasonal_lat_lon_profiles with None in ref_std_data"""
        
        seasonal_data = [self.djf, self.mam, self.jja, self.son]
        ref_data = [self.djf * 0.95, self.mam * 0.95, 
                    self.jja * 0.95, self.son * 0.95]
        
        # Introduce None in ref_std_data
        ref_std = [None, self.mam.std(), None, self.son.std()]
        
        fig, axs = plot_seasonal_lat_lon_profiles(seasonal_data=seasonal_data,
                                                ref_data=ref_data,
                                                ref_std_data=ref_std,
                                                title='Seasonal with None ref_std',
                                                data_labels=['Data'],
                                                ref_label='Reference',
                                                loglevel=loglevel)
        
        assert fig is not None
        assert axs is not None
        
        fig.savefig(tmp_path / 'test_seasonal_none_ref_std.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_seasonal_none_ref_std.png')

@pytest.mark.graphics
class TestHistogram:
    """Basic tests for the Histogram functions"""

    def setup_method(self):
        """Setup method to create histogram data for testing"""
        # Create simple histogram data with center_of_bin dimension
        bins = np.linspace(0, 10, 20)
        values = np.random.exponential(scale=2, size=20)
        
        self.hist_data = xr.DataArray(
            values,
            dims=['center_of_bin'],
            coords={'center_of_bin': bins}
        )
        self.hist_data.center_of_bin.attrs['units'] = 'm/s'
        self.hist_data.attrs['units'] = 'count'

    def test_plot_histogram_basic(self, tmp_path):
        """Test basic histogram plotting"""
        fig, ax = plot_histogram(data=self.hist_data,
                                 title='Test Histogram',
                                 loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        
        fig.savefig(tmp_path / 'test_histogram_basic.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_histogram_basic.png')

    def test_plot_histogram_multiple_with_ref(self, tmp_path):
        """Test histogram with multiple data and reference"""
        data_list = [self.hist_data, self.hist_data * 0.8]
        ref_data = self.hist_data * 1.2
        
        fig, ax = plot_histogram(data=data_list,
                                ref_data=ref_data,
                                data_labels=['Data 1', 'Data 2'],
                                ref_label='Reference',
                                smooth=True,
                                smooth_window=3,
                                xlogscale=True,
                                loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        assert len(ax.lines) == 3  # 2 data + 1 ref
        
        fig.savefig(tmp_path / 'test_histogram_multi_ref.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_histogram_multi_ref.png')

    def test_plot_histogram_no_center_of_bin(self, tmp_path):
        """Test histogram with data missing center_of_bin dimension"""
        bad_data = xr.DataArray(np.random.rand(10), dims=['time'])
        
        fig, ax = plot_histogram(data=bad_data, loglevel=loglevel)
        
        assert fig is not None
        assert ax is not None
        assert len(ax.lines) == 0  # No lines plotted
        
        fig.savefig(tmp_path / 'test_histogram_no_bins.png', dpi=DPI)
        plt.close(fig)
        assert os.path.exists(tmp_path / 'test_histogram_no_bins.png')

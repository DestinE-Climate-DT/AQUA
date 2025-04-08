import pytest
import os
import cartopy.crs as ccrs
from aqua import Reader
from aqua.graphics import plot_single_map, plot_single_map_diff
from aqua.graphics import plot_timeseries, plot_seasonalcycle
from aqua.graphics import plot_maps, plot_hovmoller

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
        fig, ax = plot_maps(maps=[plot_data, plot_data2],
                            save=True,
                            figsize=(16, 6),
                            nlevels=5,
                            vmin=-2, vmax=30,
                            sym=False,
                            cmap='viridis',
                            gridlines=True,
                            title='Test plot',
                            cbar_label='Sea surface temperature [°C]',
                            dpi=100,
                            nxticks=5,
                            nyticks=6,
                            loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_plot_maps.png')
        assert os.path.exists(tmp_path / 'test_plot_maps.png')


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

        data_labels = ['t1', 't2']

        fig, ax = plot_timeseries(monthly_data=[self.t1, self.t2], annual_data=[t1_yearly, t2_yearly],
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

        fig, ax = plot_seasonalcycle(data=t1_seasonal, ref_data=t2_seasonal,
                                     title='Seasonal cycle of temperature at two locations',
                                     data_labels='t1',
                                     ref_label='t2',
                                     loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig(tmp_path / 'test_seasonalcycle.png')

        # Check the file was created
        assert os.path.exists(tmp_path / 'test_seasonalcycle.png')


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
        fig, ax = plot_hovmoller(data=self.data,
                                 return_fig=True,
                                 outputdir=tmp_path,
                                 save=True,
                                 loglevel=loglevel)

        assert fig is not None
        assert ax is not None
        assert os.path.exists(tmp_path / 'hovmoller.pdf')

        fig2, ax2 = plot_hovmoller(data=self.data,
                                   return_fig=True,
                                   outputdir=tmp_path,
                                   filename='test_hovmoller2.png',
                                   format='png',
                                   cmap='RdBu_r',
                                   save=True,
                                   invert_axis=True,
                                   invert_time=True,
                                   cbar_label='test-label',
                                   nlevels=10,
                                   sym=True,
                                   dpi=300,
                                   loglevel=loglevel)

        assert fig2 is not None
        assert ax2 is not None
        assert os.path.exists(tmp_path / 'test_hovmoller2.png')

        plot_hovmoller(data=self.data,
                       return_fig=False,
                       contour=False,
                       outputdir=tmp_path,
                       filename='test_hovmoller3',
                       format='png',
                       cmap='RdBu_r',
                       save=True,
                       invert_time=True,
                       dpi=300,
                       loglevel=loglevel)

        assert os.path.exists(tmp_path / 'test_hovmoller3.png')

    def test_plot_hovmoller_error(self):

        with pytest.raises(TypeError):
            plot_hovmoller(data='test')

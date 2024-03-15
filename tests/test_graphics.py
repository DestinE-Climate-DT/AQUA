import pytest
import os

from aqua import Reader
from aqua.graphics import plot_single_map, plot_single_map_diff
from aqua.graphics import plot_timeseries, plot_seasonalcycle

loglevel = "DEBUG"


@pytest.mark.graphics
class TestSingleMap:
    """Basic tests for the Single map functions"""
    def setup_method(self):
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d",
                        regrid="r200", fix=False, loglevel=loglevel)
        self.data = reader.retrieve(var='sst')

    def test_plot_single_map(self):
        """
        Test the plot_single_map function
        """
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        fig, ax = plot_single_map(data=plot_data,
                                  save=True,
                                  nlevels=5,
                                  vmin=-2.0,
                                  vmax=30.0,
                                  outputdir='tests/figures/',
                                  cmap='viridis',
                                  gridlines=True,
                                  display=False,
                                  return_fig=True,
                                  title='Test plot',
                                  cbar_label='Sea surface temperature [°C]',
                                  dpi=100,
                                  model='FESOM',
                                  exp='test-pi',
                                  filename='test_single_map',
                                  format='png',
                                  nxticks=5,
                                  nyticks=5,
                                  loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        # Check the file was created
        assert os.path.exists('tests/figures/test_single_map.png')

    def test_plot_single_map_diff(self):
        """
        Test the plot_single_map_diff function
        """
        plot_data = self.data["sst"].isel(time=0).aqua.regrid()
        plot_data2 = self.data["sst"].isel(time=1).aqua.regrid()
        fig, ax = plot_single_map_diff(data=plot_data,
                                       data_ref=plot_data2,
                                       save=True,
                                       nlevels=5,
                                       vmin_fill=-5.0,
                                       vmax_fill=5.0,
                                       sym=False,
                                       vmin_contour=-2.0,
                                       vmax_contour=30.0,
                                       outputdir='tests/figures/',
                                       cmap='viridis',
                                       gridlines=True,
                                       display=False,
                                       return_fig=True,
                                       title='Test plot',
                                       cbar_label='Sea surface temperature [°C]',
                                       dpi=100,
                                       model='FESOM',
                                       exp='test-pi',
                                       filename='test_single_map_diff',
                                       format='png',
                                       nxticks=5,
                                       nyticks=5,
                                       loglevel=loglevel)
        assert fig is not None
        assert ax is not None

        # Check the file was created
        assert os.path.exists('tests/figures/test_single_map_diff.png')


@pytest.mark.graphics
class TestTimeseries:
    """Basic tests for the Timeseries functions"""

    def setup_method(self):
        model = 'IFS'
        exp = 'test-tco79'
        source = 'teleconnections'
        var = 'skt'
        self.reader = Reader(model=model, exp=exp, source=source, fix=True)
        data = self.reader.retrieve(var=var)

        self.t1 = data[var].isel(lat=1, lon=1)
        self.t2 = data[var].isel(lat=10, lon=10)

    def test_plot_timeseries(self):
        t1_yearly = self.reader.timmean(self.t1, freq='YS', center_time=True)
        t2_yearly = self.reader.timmean(self.t2, freq='YS', center_time=True)

        data_labels = ['t1', 't2']

        fig, ax = plot_timeseries(monthly_data=[self.t1, self.t2], annual_data=[t1_yearly, t2_yearly],
                                  title='Temperature at two locations',
                                  data_labels=data_labels)

        assert fig is not None
        assert ax is not None

        fig.savefig('tests/figures/test_timeseries.png')

        # Check the file was created
        assert os.path.exists('tests/figures/test_timeseries.png')

    def test_plot_seasonalcycle(self):
        t1_seasonal = self.t1.groupby('time.month').mean('time')
        t2_seasonal = self.t2.groupby('time.month').mean('time')

        fig, ax = plot_seasonalcycle(data=t1_seasonal, ref_data=t2_seasonal,
                                     title='Seasonal cycle of temperature at two locations',
                                     data_labels='t1',
                                     ref_label='t2',
                                     loglevel=loglevel)

        assert fig is not None
        assert ax is not None

        fig.savefig('tests/figures/test_seasonalcycle.png')

        # Check the file was created
        assert os.path.exists('tests/figures/test_seasonalcycle.png')

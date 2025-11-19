import pytest
import xarray as xr
import numpy as np
import matplotlib.pyplot as plt
from aqua.diagnostics.histogram import PlotHistogram
from conftest import DPI, LOGLEVEL

loglevel = LOGLEVEL

@pytest.mark.diagnostics
class TestPlotHistogram:
    """Basic tests for the PlotHistogram class"""

    def setup_method(self):
        """Setup method to create histogram data for testing"""
        # Create histogram data with proper attributes
        bins = np.linspace(250, 320, 50)
        values = np.random.exponential(scale=10, size=50)
        
        self.hist_data = xr.DataArray(
            values,
            dims=['center_of_bin'],
            coords={'center_of_bin': bins},
            attrs={
                'AQUA_catalog': 'test_catalog',
                'AQUA_model': 'IFS',
                'AQUA_exp': 'test-tco79',
                'AQUA_region': 'global',
                'short_name': 'skt',
                'standard_name': 'skin_temperature',
                'long_name': 'Skin Temperature',
                'units': 'K'
            }
        )
        self.hist_data.center_of_bin.attrs['units'] = 'K'
        
        # Create reference data
        self.ref_data = self.hist_data.copy()
        self.ref_data.attrs['AQUA_model'] = 'ERA5'
        self.ref_data.attrs['AQUA_exp'] = 'era5'

    def test_plot_histogram_initialization(self):
        """Test basic initialization"""
        plotter = PlotHistogram(data=self.hist_data, loglevel=loglevel)
        
        assert plotter.len_data == 1
        assert plotter.len_ref == 0
        assert plotter.models[0] == 'IFS'
        assert plotter.exps[0] == 'test-tco79'
        assert plotter.region == 'global'

    def test_plot_histogram_with_ref(self):
        """Test initialization with reference data"""
        plotter = PlotHistogram(
            data=self.hist_data, 
            ref_data=self.ref_data,
            loglevel=loglevel
        )
        
        assert plotter.len_data == 1
        assert plotter.len_ref == 1
        assert plotter.ref_data is not None

    def test_plot_histogram_multiple_data(self):
        """Test with multiple datasets"""
        data2 = self.hist_data.copy()
        data2.attrs['AQUA_exp'] = 'test-tco159'
        
        plotter = PlotHistogram(data=[self.hist_data, data2], loglevel=loglevel)
        
        assert plotter.len_data == 2
        assert len(plotter.models) == 2
        assert len(plotter.exps) == 2

    def test_set_labels_and_title(self):
        """Test label and title generation"""
        plotter = PlotHistogram(
            data=self.hist_data,
            ref_data=self.ref_data,
            loglevel=loglevel
        )
        
        data_labels = plotter.set_data_labels()
        ref_label = plotter.set_ref_label()
        title = plotter.set_title()
        description = plotter.set_description()
        
        assert 'IFS test-tco79' in data_labels[0]
        assert 'ERA5 era5' in ref_label
        assert 'Histogram' in title
        assert 'Skin Temperature' in title or 'skin_temperature' in title
        assert 'global' in title
        assert 'Histogram' in description

    def test_plot_basic(self):
        """Test basic plotting"""
        plotter = PlotHistogram(data=self.hist_data, loglevel=loglevel)
        
        fig, ax = plotter.plot()
        
        assert fig is not None
        assert ax is not None
        assert len(ax.lines) == 1
        plt.close(fig)

    def test_plot_with_ref_and_smooth(self):
        """Test plotting with reference and smoothing"""
        plotter = PlotHistogram(
            data=self.hist_data,
            ref_data=self.ref_data,
            loglevel=loglevel
        )
        
        fig, ax = plotter.plot(
            smooth=True,
            smooth_window=5,
            xlogscale=True,
            ylogscale=True
        )
        
        assert fig is not None
        assert ax is not None
        assert len(ax.lines) == 2  # data + ref
        plt.close(fig)

    def test_run_complete(self, tmp_path):
        """Test complete run method"""
        plotter = PlotHistogram(
            data=self.hist_data,
            ref_data=self.ref_data,
            loglevel=loglevel
        )
        
        # Just test that run completes without error
        plotter.run(
            outputdir=str(tmp_path),
            rebuild=True,
            dpi=DPI,
            format='png',
            smooth=True
        )
        
        # Verify the plot was created (fig object exists)
        assert plotter.data is not None
        assert plotter.ref_data is not None
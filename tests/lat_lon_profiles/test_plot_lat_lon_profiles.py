import pytest
import numpy as np
import xarray as xr
from aqua.diagnostics.lat_lon_profiles import PlotLatLonProfiles

loglevel = "DEBUG"


@pytest.mark.diagnostics
class TestPlotLatLonProfilesLongterm:
    """Basic tests for PlotLatLonProfiles with longterm data"""
    
    def setup_method(self):
        """Setup method to create test data"""
        # Create mock longterm data
        lat = np.linspace(-90, 90, 20)
        values = np.random.rand(20)
        
        self.data = xr.DataArray(
            values,
            dims=['lat'],
            coords={'lat': lat},
            attrs={
                'AQUA_catalog': 'test_catalog',
                'AQUA_model': 'IFS',
                'AQUA_exp': 'test-tco79',
                'AQUA_mean_type': 'zonal',
                'AQUA_region': 'Global',
                'short_name': 'skt',
                'standard_name': 'skin_temperature',
                'long_name': 'Skin Temperature',
                'units': 'K'
            }
        )
        
        self.ref_data = self.data * 0.95
        self.ref_data.attrs = self.data.attrs.copy()
        
        self.plotter = PlotLatLonProfiles(
            data=self.data,
            data_type='longterm',
            loglevel=loglevel
        )
    
    def test_initialization_longterm(self):
        """Test PlotLatLonProfiles initialization with longterm data"""
        assert self.plotter.data_type == 'longterm'
        assert len(self.plotter.data) == 1
        assert self.plotter.mean_type == 'zonal'
        assert self.plotter.region == 'Global'
    
    def test_set_data_labels(self):
        """Test setting data labels"""
        labels = self.plotter.set_data_labels()
        assert len(labels) > 0
        assert 'IFS' in labels[0]
        assert 'test-tco79' in labels[0]
    
    def test_set_ref_label(self):
        """Test setting reference label"""
        plotter = PlotLatLonProfiles(
            data=self.data,
            ref_data=self.ref_data,
            data_type='longterm',
            loglevel=loglevel
        )
        ref_label = plotter.set_ref_label()
        assert ref_label is not None
        assert 'IFS' in ref_label
    
    def test_set_title(self):
        """Test setting plot title"""
        title = self.plotter.set_title()
        assert 'Zonal profile' in title
        assert 'Global' in title
        assert 'K' in title
    
    def test_set_description(self):
        """Test setting plot description"""
        description = self.plotter.set_description()
        assert 'Zonal profile' in description
        assert 'Global' in description
    
    def test_get_data_info(self):
        """Test extracting data info"""
        assert len(self.plotter.models) == 1
        assert self.plotter.models[0] == 'IFS'
        assert len(self.plotter.exps) == 1
        assert self.plotter.exps[0] == 'test-tco79'
        assert self.plotter.short_name == 'skt'
        assert self.plotter.units == 'K'
    
    def test_plot_longterm(self, tmp_path):
        """Test plotting longterm data"""
        data_labels = self.plotter.set_data_labels()
        title = self.plotter.set_title()
        
        fig, ax = self.plotter.plot(
            data_labels=data_labels,
            title=title
        )
        
        assert fig is not None
        assert ax is not None
        
        # Save to verify it works
        fig.savefig(tmp_path / 'test_longterm_plot.png')
        assert (tmp_path / 'test_longterm_plot.png').exists()
    
    def test_plot_with_reference(self, tmp_path):
        """Test plotting with reference data"""
        plotter = PlotLatLonProfiles(
            data=self.data,
            ref_data=self.ref_data,
            data_type='longterm',
            loglevel=loglevel
        )
        
        fig, ax = plotter.plot()
        
        assert fig is not None
        assert ax is not None
        
        fig.savefig(tmp_path / 'test_with_ref.png')
        assert (tmp_path / 'test_with_ref.png').exists()
    
    def test_save_plot(self, tmp_path):
        """Test saving plot to file"""
        fig, _ = self.plotter.plot()
        
        self.plotter.save_plot(
            fig=fig,
            description='Test description',
            outputdir=str(tmp_path),
            rebuild=True,
            format='png'
        )
        
        files = list(tmp_path.rglob('*.png'))
        assert len(files) > 0
    
    def test_run_longterm(self, tmp_path):
        """Test full run method for longterm data"""
        self.plotter.run(
            outputdir=str(tmp_path),
            rebuild=True,
            format='png'
        )
        
        files = list(tmp_path.rglob('*.png'))
        assert len(files) > 0


@pytest.mark.diagnostics
class TestPlotLatLonProfilesSeasonal:
    """Basic tests for PlotLatLonProfiles with seasonal data"""
    
    def setup_method(self):
        """Setup method to create seasonal test data"""
        lat = np.linspace(-90, 90, 20)
        
        # Create seasonal data (DJF, MAM, JJA, SON)
        self.seasonal_data = []
        for i in range(4):
            values = np.random.rand(20) + i * 0.1
            data = xr.DataArray(
                values,
                dims=['lat'],
                coords={'lat': lat},
                attrs={
                    'AQUA_catalog': 'test_catalog',
                    'AQUA_model': 'IFS',
                    'AQUA_exp': 'test-tco79',
                    'AQUA_mean_type': 'zonal',
                    'AQUA_region': 'Tropics',
                    'short_name': 'skt',
                    'standard_name': 'skin_temperature',
                    'long_name': 'Skin Temperature',
                    'units': 'K'
                }
            )
            self.seasonal_data.append(data)
        
        self.plotter = PlotLatLonProfiles(
            data=self.seasonal_data,
            data_type='seasonal',
            loglevel=loglevel
        )
    
    def test_initialization_seasonal(self):
        """Test PlotLatLonProfiles initialization with seasonal data"""
        assert self.plotter.data_type == 'seasonal'
        assert len(self.plotter.data) == 4
        assert self.plotter.mean_type == 'zonal'
    
    def test_plot_seasonal(self, tmp_path):
        """Test plotting seasonal data"""
        data_labels = self.plotter.set_data_labels()
        title = self.plotter.set_title()
        
        fig, axs = self.plotter.plot_seasonal_lines(
            data_labels=data_labels,
            title=title
        )
        
        assert fig is not None
        assert axs is not None
        assert len(axs) == 4  # 4 panels for seasons
        
        fig.savefig(tmp_path / 'test_seasonal_plot.png')
        assert (tmp_path / 'test_seasonal_plot.png').exists()
    
    def test_plot_seasonal_with_reference(self, tmp_path):
        """Test plotting seasonal data with reference"""
        ref_data = [d * 0.95 for d in self.seasonal_data]
        
        plotter = PlotLatLonProfiles(
            data=self.seasonal_data,
            ref_data=ref_data,
            data_type='seasonal',
            loglevel=loglevel
        )
        
        fig, axs = plotter.plot_seasonal_lines()
        
        assert fig is not None
        assert len(axs) == 4
        
        fig.savefig(tmp_path / 'test_seasonal_with_ref.png')
        assert (tmp_path / 'test_seasonal_with_ref.png').exists()
    
    def test_run_seasonal(self, tmp_path):
        """Test full run method for seasonal data"""
        self.plotter.run(
            outputdir=str(tmp_path),
            rebuild=True,
            format='png'
        )
        
        files = list(tmp_path.rglob('*.png'))
        assert len(files) > 0
    
    def test_seasonal_insufficient_data(self):
        """Test error when seasonal data has insufficient elements"""
        plotter = PlotLatLonProfiles(
            data=self.seasonal_data[:2],  # Only 2 seasons
            data_type='seasonal',
            loglevel=loglevel
        )
        
        with pytest.raises(ValueError, match='must contain at least 4 elements'):
            plotter.plot_seasonal_lines()
            
    def test_plot_method_delegates_to_seasonal(self, tmp_path):
        """Test that plot() method correctly delegates to plot_seasonal_lines() for seasonal data"""

        data_labels = self.plotter.set_data_labels()
        title = self.plotter.set_title()
        fig, axs = self.plotter.plot(
            data_labels=data_labels,
            title=title
        )
        
        # Verify it returns the correct 4-panel seasonal plot
        assert fig is not None
        assert axs is not None
        assert len(axs) == 4  # 4 panels for seasons
        
        fig.savefig(tmp_path / 'test_plot_delegates_seasonal.png')
        assert (tmp_path / 'test_plot_delegates_seasonal.png').exists()
    
    def test_seasonal_with_ref_std_data(self, tmp_path):
        """Test seasonal plotting with reference std data"""
        ref_data = [d * 0.95 for d in self.seasonal_data]
        
        # Create ref_std_data with std dates
        ref_std = self.seasonal_data[0] * 0.1
        ref_std.attrs = self.seasonal_data[0].attrs.copy()
        ref_std.attrs['std_startdate'] = '1990-01-01'
        ref_std.attrs['std_enddate'] = '1995-12-31'
        
        plotter = PlotLatLonProfiles(
            data=self.seasonal_data,
            ref_data=ref_data,
            ref_std_data=ref_std,
            data_type='seasonal',
            loglevel=loglevel
        )
        
        assert plotter.std_startdate == '1990-01-01'
        assert plotter.std_enddate == '1995-12-31'
        
        # Test description includes std info
        description = plotter.set_description()
        assert 'standard deviation' in description or '1990' in description
        
        fig, axs = plotter.plot_seasonal_lines()
        assert fig is not None
        assert len(axs) == 4
        
        fig.savefig(tmp_path / 'test_seasonal_with_std.png')
        assert (tmp_path / 'test_seasonal_with_std.png').exists()

@pytest.mark.diagnostics
class TestPlotLatLonProfilesMeridional:
    """Tests for PlotLatLonProfiles with meridional mean"""
    
    def setup_method(self):
        """Setup method to create meridional test data"""
        lon = np.linspace(0, 360, 30)
        values = np.random.rand(30)
        
        self.data = xr.DataArray(
            values,
            dims=['lon'],
            coords={'lon': lon},
            attrs={
                'AQUA_catalog': 'test_catalog',
                'AQUA_model': 'IFS',
                'AQUA_exp': 'test-tco79',
                'AQUA_mean_type': 'meridional',
                'short_name': 'skt',
                'units': 'K'
            }
        )
        
        self.plotter = PlotLatLonProfiles(
            data=self.data,
            data_type='longterm',
            loglevel=loglevel
        )
    
    def test_meridional_initialization(self):
        """Test initialization with meridional data"""
        assert self.plotter.mean_type == 'meridional'
        assert self.plotter.data_type == 'longterm'
    
    def test_meridional_title(self):
        """Test title for meridional plot"""
        title = self.plotter.set_title()
        assert 'Meridional profile' in title
    
    def test_plot_meridional(self, tmp_path):
        """Test plotting meridional data"""
        fig, ax = self.plotter.plot()
        
        assert fig is not None
        assert ax is not None
        
        fig.savefig(tmp_path / 'test_meridional_plot.png')
        assert (tmp_path / 'test_meridional_plot.png').exists()


@pytest.mark.diagnostics
class TestPlotLatLonProfilesMultipleModels:
    """Tests for PlotLatLonProfiles with multiple models"""
    
    def setup_method(self):
        """Setup method to create multiple model data"""
        lat = np.linspace(-90, 90, 20)
        
        self.data_list = []
        for i, (model, exp) in enumerate([('IFS', 'test-tco79'), ('GFS', 'test-exp2')]):
            values = np.random.rand(20) + i * 0.1
            data = xr.DataArray(
                values,
                dims=['lat'],
                coords={'lat': lat},
                attrs={
                    'AQUA_catalog': f'catalog_{i}',
                    'AQUA_model': model,
                    'AQUA_exp': exp,
                    'AQUA_mean_type': 'zonal',
                    'short_name': 'skt',
                    'units': 'K'
                }
            )
            self.data_list.append(data)
        
        self.plotter = PlotLatLonProfiles(
            data=self.data_list,
            data_type='longterm',
            loglevel=loglevel
        )
    
    def test_multiple_models_initialization(self):
        """Test initialization with multiple models"""
        assert len(self.plotter.data) == 2
        assert len(self.plotter.models) == 2
        assert 'IFS' in self.plotter.models
        assert 'GFS' in self.plotter.models
    
    def test_multiple_models_labels(self):
        """Test labels for multiple models"""
        labels = self.plotter.set_data_labels()
        assert len(labels) == 2
        assert 'IFS test-tco79' in labels
        assert 'GFS test-exp2' in labels
    
    def test_plot_multiple_models(self, tmp_path):
        """Test plotting multiple models"""
        fig, ax = self.plotter.plot()
        
        assert fig is not None
        assert ax is not None
        assert len(ax.lines) >= 2  # At least 2 lines plotted
        
        fig.savefig(tmp_path / 'test_multiple_models.png')
        assert (tmp_path / 'test_multiple_models.png').exists()


@pytest.mark.diagnostics
class TestPlotLatLonProfilesErrors:
    """Test error handling in PlotLatLonProfiles"""
    
    def test_invalid_data_type(self):
        """Test that invalid data_type raises error"""
        lat = np.linspace(-90, 90, 20)
        data = xr.DataArray(np.random.rand(20), dims=['lat'], coords={'lat': lat})
        
        with pytest.raises(ValueError, match="data_type must be 'longterm' or 'seasonal'"):
            PlotLatLonProfiles(data=data, data_type='invalid', loglevel=loglevel)
    
    def test_save_plot_pdf_format(self, tmp_path):
        """Test saving plot in PDF format"""
        lat = np.linspace(-90, 90, 20)
        data = xr.DataArray(
            np.random.rand(20),
            dims=['lat'],
            coords={'lat': lat},
            attrs={
                'AQUA_catalog': 'test',
                'AQUA_model': 'IFS',
                'AQUA_exp': 'test',
                'AQUA_mean_type': 'zonal',
                'short_name': 'skt'
            }
        )
        
        plotter = PlotLatLonProfiles(data=data, data_type='longterm', loglevel=loglevel)
        fig, _ = plotter.plot()
        
        plotter.save_plot(
            fig=fig,
            description='Test PDF',
            outputdir=str(tmp_path),
            rebuild=True,
            format='pdf'
        )
        
        files = list(tmp_path.rglob('*.pdf'))
        assert len(files) > 0
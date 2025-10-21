import pytest
import numpy as np
import xarray as xr
from aqua.diagnostics.lat_lon_profiles import PlotLatLonProfiles

loglevel = "DEBUG"

@pytest.fixture
def sample_lat_lon_data():
    """Factory fixture for creating test data"""
    def _make_data(mean_type='zonal', num_datasets=1, seasonal=False):
        coord_name = 'lat' if mean_type == 'zonal' else 'lon'
        coord_values = np.linspace(-90, 90, 20) if mean_type == 'zonal' else np.linspace(0, 360, 30)
        
        def make_single_array(i=0):
            return xr.DataArray(
                np.random.rand(len(coord_values)) + i * 0.1,
                dims=[coord_name],
                coords={coord_name: coord_values},
                attrs={
                    'AQUA_catalog': f'catalog_{i}' if num_datasets > 1 else 'test_catalog',
                    'AQUA_model': ['IFS', 'GFS', 'ECMWF'][i % 3] if num_datasets > 1 else 'IFS',
                    'AQUA_exp': f'test-exp{i}' if num_datasets > 1 else 'test-tco79',
                    'AQUA_mean_type': mean_type,
                    'AQUA_region': 'Global',
                    'short_name': 'skt',
                    'standard_name': 'skin_temperature',
                    'long_name': 'Skin Temperature',
                    'units': 'K'
                }
            )
        
        if seasonal:
            return [make_single_array(i) for i in range(4)]
        elif num_datasets > 1:
            return [make_single_array(i) for i in range(num_datasets)]
        else:
            return make_single_array()
    
    return _make_data

@pytest.mark.diagnostics
class TestPlotLatLonProfilesCore:
    """Core functionality tests - covers most scenarios efficiently"""
    
    @pytest.mark.parametrize("mean_type", ['zonal', 'meridional'])
    def test_initialization_and_metadata(self, sample_lat_lon_data, mean_type):
        """Test initialization and metadata extraction for both mean types"""
        data = sample_lat_lon_data(mean_type=mean_type)
        plotter = PlotLatLonProfiles(data=data, data_type='longterm', loglevel=loglevel)
        
        assert plotter.data_type == 'longterm'
        assert plotter.mean_type == mean_type
        assert plotter.region == 'Global'
        assert plotter.diagnostic_name == 'lat_lon_profiles'  # default
        
        # Test title generation
        title = plotter.set_title()
        assert mean_type.capitalize() in title
        
    def test_custom_diagnostic_name(self, sample_lat_lon_data):
        """Test custom diagnostic_name parameter"""
        data = sample_lat_lon_data()
        custom_name = 'my_custom_profile'
        
        plotter = PlotLatLonProfiles(
            data=data, 
            data_type='longterm',
            diagnostic_name=custom_name,
            loglevel=loglevel
        )
        
        assert plotter.diagnostic_name == custom_name
    
    @pytest.mark.parametrize("num_datasets", [1, 2, 3])
    def test_multiple_datasets(self, sample_lat_lon_data, num_datasets):
        """Test handling of multiple datasets"""
        data = sample_lat_lon_data(num_datasets=num_datasets)
        plotter = PlotLatLonProfiles(data=data, data_type='longterm', loglevel=loglevel)
        
        assert len(plotter.data) == num_datasets
        assert len(plotter.models) == num_datasets
        
        labels = plotter.set_data_labels()
        assert len(labels) == num_datasets
    
    def test_reference_data(self, sample_lat_lon_data):
        """Test with reference data"""
        data = sample_lat_lon_data()
        ref_data = data * 0.95
        ref_data.attrs = data.attrs.copy()
        
        plotter = PlotLatLonProfiles(
            data=data, 
            ref_data=ref_data,
            data_type='longterm',
            loglevel=loglevel
        )
        
        ref_label = plotter.set_ref_label()
        assert ref_label is not None
        assert 'IFS' in ref_label


@pytest.mark.diagnostics  
class TestPlotLatLonProfilesSeasonal:
    """Seasonal-specific tests"""
    
    def test_seasonal_initialization(self, sample_lat_lon_data):
        """Test seasonal data initialization"""
        seasonal_data = sample_lat_lon_data(seasonal=True)
        
        plotter = PlotLatLonProfiles(
            data=seasonal_data,
            data_type='seasonal',
            loglevel=loglevel
        )
        
        assert plotter.data_type == 'seasonal'
        assert len(plotter.data) == 4
    
    def test_seasonal_insufficient_data(self, sample_lat_lon_data):
        """Test error when seasonal data has insufficient elements"""
        seasonal_data = sample_lat_lon_data(seasonal=True)[:2]  # Only 2 seasons
        
        plotter = PlotLatLonProfiles(
            data=seasonal_data,
            data_type='seasonal',
            loglevel=loglevel
        )
        
        with pytest.raises(ValueError, match='must contain at least 4 elements'):
            plotter.plot_seasonal_lines()


@pytest.mark.diagnostics
class TestPlotLatLonProfilesIntegration:
    """Integration tests - actual plotting and file saving"""
    
    @pytest.mark.parametrize("data_type,format", [
        ('longterm', 'png'),
        ('longterm', 'pdf'),
        ('seasonal', 'png'),
    ])
    def test_full_run(self, sample_lat_lon_data, tmp_path, data_type, format):
        """Test complete run with file output"""
        seasonal = (data_type == 'seasonal')
        data = sample_lat_lon_data(seasonal=seasonal)
        
        plotter = PlotLatLonProfiles(
            data=data,
            data_type=data_type,
            loglevel=loglevel
        )
        
        plotter.run(
            outputdir=str(tmp_path),
            rebuild=True,
            format=format
        )
        
        files = list(tmp_path.rglob(f'*.{format}'))
        assert len(files) > 0, f"No {format} files created"
        assert files[0].stat().st_size > 0, f"{format.upper()} file is empty"
    
    def test_custom_diagnostic_name_in_output(self, sample_lat_lon_data, tmp_path):
        """Test that custom diagnostic_name affects output"""
        data = sample_lat_lon_data()
        custom_name = 'custom_profile_test'
        
        plotter = PlotLatLonProfiles(
            data=data,
            data_type='longterm',
            diagnostic_name=custom_name,
            loglevel=loglevel
        )
        
        plotter.run(outputdir=str(tmp_path), rebuild=True, format='png')
        
        files = list(tmp_path.rglob('*.png'))
        assert len(files) > 0


@pytest.mark.diagnostics
class TestPlotLatLonProfilesErrors:
    """Error handling tests"""
    
    def test_invalid_data_type(self, sample_lat_lon_data):
        """Test that invalid data_type raises error"""
        data = sample_lat_lon_data()
        
        with pytest.raises(ValueError, match="data_type must be 'longterm' or 'seasonal'"):
            PlotLatLonProfiles(data=data, data_type='invalid', loglevel=loglevel)
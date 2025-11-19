import pytest
from aqua.diagnostics.histogram import Histogram

loglevel = "DEBUG"


@pytest.mark.diagnostics
class TestHistogram:
    """Basic tests for the Histogram diagnostic class"""

    def setup_method(self):
        """Setup method to initialize Histogram instance"""
        self.hist = Histogram(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1990-01-01',
            enddate='1991-12-31',
            bins=50,
            weighted=True,
            loglevel=loglevel
        )

    def test_histogram_initialization(self):
        """Test basic initialization"""
        assert self.hist.model == 'IFS'
        assert self.hist.exp == 'test-tco79'
        assert self.hist.bins == 50
        assert self.hist.weighted is True
        assert self.hist.histogram_data is None

    def test_retrieve_and_compute(self, tmp_path):
        """Test retrieve and compute_histogram methods"""
        self.hist.retrieve(var='skt')
        
        assert self.hist.data is not None
        assert 'time' in self.hist.data.dims
        
        self.hist.compute_histogram(density=True)
        
        assert self.hist.histogram_data is not None
        assert 'center_of_bin' in self.hist.histogram_data.dims
        assert len(self.hist.histogram_data.center_of_bin) == self.hist.bins

    def test_full_run(self, tmp_path):
        """Test complete run method"""
        self.hist.run(
            var='skt',
            outputdir=str(tmp_path),
            rebuild=True,
            density=True
        )
        
        assert self.hist.histogram_data is not None
        assert 'center_of_bin' in self.hist.histogram_data.dims

    def test_histogram_with_region(self):
        """Test histogram with specific region from file"""
        hist_regional = Histogram(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            region='tropics',
            bins=30,
            loglevel=loglevel
        )
        
        hist_regional.retrieve(var='skt')
        hist_regional.compute_histogram()
        
        assert hist_regional.histogram_data is not None
        assert hist_regional.region == 'Tropics'
        assert hist_regional.lon_limits == [-180, 180]
        assert hist_regional.lat_limits == [-15, 15]

    def test_histogram_with_formula(self):
        """Test histogram with formula evaluation"""
        self.hist.retrieve(
            var='skt+273.15',
            formula=True,
            long_name='Temperature',
            units='K',
            standard_name='temperature'
        )
        
        assert self.hist.data is not None
        assert self.hist.data.attrs['units'] == 'K'

    def test_histogram_custom_range(self):
        """Test histogram with custom range"""
        hist_custom = Histogram(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            bins=25,
            range=(250, 320),
            loglevel=loglevel
        )
        
        hist_custom.retrieve(var='skt')
        hist_custom.compute_histogram()
        
        assert hist_custom.histogram_data is not None
        bin_min = float(hist_custom.histogram_data.center_of_bin.min())
        bin_max = float(hist_custom.histogram_data.center_of_bin.max())
        assert bin_min >= 250
        assert bin_max <= 320

    def test_histogram_counts_not_density(self):
        """Test histogram with counts (not density)"""
        self.hist.retrieve(var='skt')
        self.hist.compute_histogram(density=False)
        
        assert self.hist.histogram_data is not None
        assert self.hist.histogram_data.name == 'histogram'
        assert self.hist.histogram_data.attrs['units'] == 'counts'
        assert 'Histogram of' in self.hist.histogram_data.attrs.get('long_name', '')

    def test_histogram_auto_dates(self):
        """Test histogram with automatic date detection"""
        hist_auto_dates = Histogram(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate=None,  # Will use data min
            enddate=None,    # Will use data max
            bins=40,
            loglevel=loglevel
        )
        
        hist_auto_dates.retrieve(var='skt')
        
        # Check that dates were set from data
        assert hist_auto_dates.startdate is not None
        assert hist_auto_dates.enddate is not None
        
        hist_auto_dates.compute_histogram()
        assert hist_auto_dates.histogram_data is not None

    def test_error_invalid_variable(self):
        """Test error handling for invalid variable"""
        with pytest.raises(ValueError, match='nonexistent_var'):
            self.hist.retrieve(var='nonexistent_var')

    def test_save_netcdf_without_data(self, tmp_path):
        """Test save_netcdf error when no data computed"""
        # Don't run retrieve/compute
        self.hist.save_netcdf(outputdir=str(tmp_path))
        # Should log error and return without raising exception
        assert self.hist.histogram_data is None
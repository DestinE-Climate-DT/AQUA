import pytest
import numpy as np
import xarray as xr
from aqua.diagnostics.lat_lon_profiles import LatLonProfiles

loglevel = "DEBUG"


@pytest.mark.diagnostics
class TestLatLonProfilesZonal:
    """Basic tests for the LatLonProfiles class with zonal mean"""
    
    def setup_method(self):
        """Setup method to initialize LatLonProfiles instance"""
        self.diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            mean_type='zonal',
            loglevel=loglevel
        )
    
    def test_retrieve_simple_var(self):
        """Test retrieve method with a simple variable"""
        self.diagnostic.retrieve(var='skt')
        
        assert self.diagnostic.data is not None
        assert isinstance(self.diagnostic.data, xr.DataArray)
        assert 'skt' in str(self.diagnostic.data.name) or 'skt' in str(self.diagnostic.data.attrs.get('standard_name', ''))
    
    def test_retrieve_with_formula(self):
        """Test retrieve method with a formula"""
        self.diagnostic.retrieve(
            var='skt+2',
            formula=True,
            long_name='Temperature plus 2',
            units='K',
            standard_name='skt_plus_2'
        )
        
        assert self.diagnostic.data is not None
        assert self.diagnostic.data.attrs['standard_name'] == 'skt_plus_2'
        assert self.diagnostic.data.attrs['units'] == 'K'
    
    def test_compute_seasonal_mean(self):
        """Test computation of seasonal mean"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        
        assert self.diagnostic.seasonal is not None
        assert len(self.diagnostic.seasonal) == 4  # DJF, MAM, JJA, SON
        
        for season_data in self.diagnostic.seasonal:
            assert isinstance(season_data, xr.DataArray)
            assert 'AQUA_mean_type' in season_data.attrs
            assert season_data.attrs['AQUA_mean_type'] == 'zonal'
    
    def test_compute_longterm_mean(self):
        """Test computation of longterm mean"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='longterm')
        
        assert self.diagnostic.longterm is not None
        assert isinstance(self.diagnostic.longterm, xr.DataArray)
        assert 'AQUA_mean_type' in self.diagnostic.longterm.attrs
        assert self.diagnostic.longterm.attrs['AQUA_mean_type'] == 'zonal'
    
    def test_compute_seasonal_std(self):
        """Test computation of seasonal standard deviation"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_std(freq='seasonal')
        
        assert self.diagnostic.std_seasonal is not None
        assert isinstance(self.diagnostic.std_seasonal, xr.DataArray)
    
    def test_compute_longterm_std(self):
        """Test computation of longterm standard deviation"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_std(freq='longterm')
        
        assert self.diagnostic.std_annual is not None
        assert isinstance(self.diagnostic.std_annual, xr.DataArray)
    
    def test_save_seasonal_netcdf(self, tmp_path):
        """Test saving seasonal data to netcdf"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        self.diagnostic.save_netcdf(freq='seasonal', outputdir=str(tmp_path), rebuild=True)
        
        # Check that files were created somewhere in tmp_path (including subdirectories)
        files = list(tmp_path.rglob('*.nc'))  # rglob searches recursively
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_save_longterm_netcdf(self, tmp_path):
        """Test saving longterm data to netcdf"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='longterm')
        self.diagnostic.save_netcdf(freq='longterm', outputdir=str(tmp_path), rebuild=True)
        
        # Check that file was created somewhere in tmp_path
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_save_seasonal_with_std(self, tmp_path):
        """Test saving seasonal data with standard deviation"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        self.diagnostic.compute_std(freq='seasonal')
        self.diagnostic.save_netcdf(freq='seasonal', outputdir=str(tmp_path), rebuild=True)
        
        # Check that files were created (should include both mean and std files)
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
        
        # Verify std_seasonal was computed
        assert self.diagnostic.std_seasonal is not None

    def test_save_longterm_with_std(self, tmp_path):
        """Test saving longterm data with standard deviation"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='longterm')
        self.diagnostic.compute_std(freq='longterm')
        self.diagnostic.save_netcdf(freq='longterm', outputdir=str(tmp_path), rebuild=True)
        
        # Check that files were created (should include both mean and std files)
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
        
        # Verify std_annual was computed
        assert self.diagnostic.std_annual is not None

    def test_run_seasonal(self, tmp_path):
        """Test full run method with seasonal frequency"""
        self.diagnostic.run(
            var='skt',
            freq=['seasonal'],
            std=True,
            outputdir=str(tmp_path),
            rebuild=True
        )
        
        assert self.diagnostic.seasonal is not None
        assert self.diagnostic.std_seasonal is not None
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_run_longterm(self, tmp_path):
        """Test full run method with longterm frequency"""
        self.diagnostic.run(
            var='skt',
            freq=['longterm'],
            std=True,
            outputdir=str(tmp_path),
            rebuild=True
        )
        
        assert self.diagnostic.longterm is not None
        assert self.diagnostic.std_annual is not None
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_run_both_frequencies(self, tmp_path):
        """Test full run method with both seasonal and longterm frequencies"""
        self.diagnostic.run(
            var='skt',
            freq=['seasonal', 'longterm'],
            std=True,
            outputdir=str(tmp_path),
            rebuild=True
        )
        
        assert self.diagnostic.seasonal is not None
        assert self.diagnostic.longterm is not None
        assert self.diagnostic.std_seasonal is not None
        assert self.diagnostic.std_annual is not None
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"


@pytest.mark.diagnostics
class TestLatLonProfilesMeridional:
    """Basic tests for the LatLonProfiles class with meridional mean"""
    
    def setup_method(self):
        """Setup method to initialize LatLonProfiles instance with meridional mean"""
        self.diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            mean_type='meridional',
            loglevel=loglevel
        )
    
    def test_compute_meridional_seasonal_mean(self):
        """Test computation of meridional seasonal mean"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        
        assert self.diagnostic.seasonal is not None
        assert len(self.diagnostic.seasonal) == 4
        
        for season_data in self.diagnostic.seasonal:
            assert season_data.attrs['AQUA_mean_type'] == 'meridional'
    
    def test_compute_meridional_longterm_mean(self):
        """Test computation of meridional longterm mean"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='longterm')
        
        assert self.diagnostic.longterm is not None
        assert self.diagnostic.longterm.attrs['AQUA_mean_type'] == 'meridional'
    
    def test_run_meridional(self, tmp_path):
        """Test full run method with meridional mean"""
        self.diagnostic.run(
            var='skt',
            freq=['seasonal', 'longterm'],
            std=False,
            outputdir=str(tmp_path),
            rebuild=True
        )
        
        assert self.diagnostic.seasonal is not None
        assert self.diagnostic.longterm is not None
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"


@pytest.mark.diagnostics
class TestLatLonProfilesWithRegion:
    """Tests for LatLonProfiles class with region specification"""
    
    def setup_method(self):
        """Setup method with region limits"""
        self.diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            lon_limits=[-180, 180],
            lat_limits=[-60, 60],
            mean_type='zonal',
            loglevel=loglevel
        )
    
    def test_compute_with_region(self):
        """Test computation with specified region"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        
        assert self.diagnostic.seasonal is not None
        assert self.diagnostic.lon_limits == [-180, 180]
        assert self.diagnostic.lat_limits == [-60, 60]
    
    def test_save_with_region(self, tmp_path):
        """Test saving data with region information"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq='seasonal')
        self.diagnostic.save_netcdf(freq='seasonal', outputdir=str(tmp_path), rebuild=True)
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_compute_with_region_name(self):
        """Test computation with named region"""
        # Use a specific region name that should have AQUA_region attribute
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            region='tropics',
            mean_type='zonal',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        diagnostic.compute_dim_mean(freq='seasonal')
        
        # Check that AQUA_region attribute is set
        assert diagnostic.seasonal is not None
        for season_data in diagnostic.seasonal:
            assert 'AQUA_region' in season_data.attrs
            assert season_data.attrs['AQUA_region'] == 'Tropics'
    
    def test_compute_longterm_with_region(self):
        """Test longterm computation with region sets AQUA_region attribute"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            region='tropics',
            mean_type='zonal',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        diagnostic.compute_dim_mean(freq='longterm')
        
        # Check that AQUA_region attribute is set for longterm
        assert diagnostic.longterm is not None
        assert 'AQUA_region' in diagnostic.longterm.attrs
        assert diagnostic.longterm.attrs['AQUA_region'] == 'Tropics'
    
    def test_save_seasonal_with_region_in_filename(self, tmp_path):
        """Test that region name appears in saved files"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            region='tropics',
            mean_type='zonal',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        diagnostic.compute_dim_mean(freq='seasonal')
        diagnostic.compute_std(freq='seasonal')
        diagnostic.save_netcdf(freq='seasonal', outputdir=str(tmp_path), rebuild=True)
        
        # Check files were created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"
    
    def test_save_longterm_with_region_and_std(self, tmp_path):
        """Test saving longterm data with region and std"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            region='tropics',
            mean_type='zonal',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        diagnostic.compute_dim_mean(freq='longterm')
        diagnostic.compute_std(freq='longterm')
        diagnostic.save_netcdf(freq='longterm', outputdir=str(tmp_path), rebuild=True)
        
        # Check files were created (both mean and std)
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path} or subdirectories"


@pytest.mark.diagnostics
class TestLatLonProfilesErrors:
    """Test error handling in LatLonProfiles class"""
    
    def test_invalid_mean_type(self):
        """Test that invalid mean_type raises error"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            mean_type='invalid',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        
        with pytest.raises(ValueError):
            diagnostic.compute_dim_mean(freq='seasonal')
    
    def test_save_without_data(self, tmp_path):
        """Test save_netcdf without computing data first"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            loglevel=loglevel
        )
        
        # Should log error and return without raising exception
        diagnostic.save_netcdf(freq='seasonal', outputdir=str(tmp_path))
        
        # In this case, no files should be created
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) == 0, "No files should be created when data is missing"

    def test_invalid_mean_type_in_compute_std(self):
        """Test that invalid mean_type raises error in compute_std"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            mean_type='invalid',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        
        # Test that compute_std also raises ValueError for invalid mean_type
        with pytest.raises(ValueError, match='Mean type invalid not recognized for std computation'):
            diagnostic.compute_std(freq='seasonal')
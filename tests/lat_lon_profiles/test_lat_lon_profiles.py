import pytest
import numpy as np
import xarray as xr
from aqua.diagnostics.lat_lon_profiles import LatLonProfiles
from conftest import LOGLEVEL

loglevel = LOGLEVEL


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
    
    def _assert_files_created(self, tmp_path, min_files=1):
        """Helper to check that files were created"""
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) >= min_files, f"Expected at least {min_files} .nc files in {tmp_path}"
        return files
    
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
    
    @pytest.mark.parametrize("freq,attr_name", [
        ('seasonal', 'seasonal'),
        ('longterm', 'longterm')
    ])
    def test_compute_dim_mean(self, freq, attr_name):
        """Test computation of dimensional mean for different frequencies"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq=freq)
        
        data = getattr(self.diagnostic, attr_name)
        assert data is not None
        
        if freq == 'seasonal':
            assert len(data) == 4  # DJF, MAM, JJA, SON
            for season_data in data:
                assert isinstance(season_data, xr.DataArray)
                assert 'AQUA_mean_type' in season_data.attrs
                assert season_data.attrs['AQUA_mean_type'] == 'zonal'
        else:
            assert isinstance(data, xr.DataArray)
            assert 'AQUA_mean_type' in data.attrs
            assert data.attrs['AQUA_mean_type'] == 'zonal'
    
    @pytest.mark.parametrize("freq,std_attr", [
        ('seasonal', 'std_seasonal'),
        ('longterm', 'std_annual')
    ])
    def test_compute_std(self, freq, std_attr):
        """Test computation of standard deviation for different frequencies"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_std(freq=freq)
        
        std_data = getattr(self.diagnostic, std_attr)
        assert std_data is not None
        # 4 element of DataArray list for seasonal, single DataArray for longterm
        if freq == 'seasonal':
            assert len(std_data) == 4
            for season_std in std_data:
                assert isinstance(season_std, xr.DataArray)
        elif freq == 'longterm':
            assert isinstance(std_data, xr.DataArray)
    
    @pytest.mark.parametrize("freq,with_std", [
        ('seasonal', False),
        ('seasonal', True),
        ('longterm', False),
        ('longterm', True)
    ])
    def test_save_netcdf(self, tmp_path, freq, with_std):
        """Test saving data to netcdf with different frequencies and std options"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq=freq)
        
        if with_std:
            self.diagnostic.compute_std(freq=freq)
        
        self.diagnostic.save_netcdf(freq=freq, outputdir=str(tmp_path), rebuild=True)
        
        # Verify files were created
        self._assert_files_created(tmp_path)
        
        # Verify std data exists if requested
        if with_std:
            std_attr = 'std_seasonal' if freq == 'seasonal' else 'std_annual'
            assert getattr(self.diagnostic, std_attr) is not None

    @pytest.mark.parametrize("freq", ['seasonal', 'longterm', ['seasonal', 'longterm']])
    def test_run(self, tmp_path, freq):
        """Test full run method with different frequency options"""
        self.diagnostic.run(
            var='skt',
            freq=freq if isinstance(freq, list) else [freq],
            std=True,
            outputdir=str(tmp_path),
            rebuild=True
        )
        
        # Check that appropriate data was computed
        freq_list = freq if isinstance(freq, list) else [freq]
        
        for f in freq_list:
            if f == 'seasonal':
                assert self.diagnostic.seasonal is not None
                assert self.diagnostic.std_seasonal is not None
            elif f == 'longterm':
                assert self.diagnostic.longterm is not None
                assert self.diagnostic.std_annual is not None
        
        # Verify files were created
        self._assert_files_created(tmp_path)


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
    
    def _assert_files_created(self, tmp_path):
        """Helper to check that files were created"""
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path}"
        return files
    
    @pytest.mark.parametrize("freq", ['seasonal', 'longterm'])
    def test_compute_meridional_mean(self, freq):
        """Test computation of meridional mean for different frequencies"""
        self.diagnostic.retrieve(var='skt')
        self.diagnostic.compute_dim_mean(freq=freq)
        
        if freq == 'seasonal':
            assert self.diagnostic.seasonal is not None
            assert len(self.diagnostic.seasonal) == 4
            for season_data in self.diagnostic.seasonal:
                assert season_data.attrs['AQUA_mean_type'] == 'meridional'
        else:
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
        self._assert_files_created(tmp_path)


@pytest.mark.diagnostics
class TestLatLonProfilesWithRegion:
    """Tests for LatLonProfiles class with region specification"""
    
    def _assert_files_created(self, tmp_path):
        """Helper to check that files were created"""
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0, f"No .nc files found in {tmp_path}"
        return files
    
    def test_compute_with_region_limits(self):
        """Test computation with specified region limits"""
        diagnostic = LatLonProfiles(
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
        
        diagnostic.retrieve(var='skt')
        diagnostic.compute_dim_mean(freq='seasonal')
        
        assert diagnostic.seasonal is not None
        assert diagnostic.lon_limits == [-180, 180]
        assert diagnostic.lat_limits == [-60, 60]
    
    @pytest.mark.parametrize("freq", ['seasonal', 'longterm'])
    def test_compute_with_region_name(self, freq):
        """Test computation with named region sets AQUA_region attribute"""
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
        diagnostic.compute_dim_mean(freq=freq)
        
        if freq == 'seasonal':
            assert diagnostic.seasonal is not None
            for season_data in diagnostic.seasonal:
                assert 'AQUA_region' in season_data.attrs
                assert season_data.attrs['AQUA_region'] == 'Tropics'
        else:
            assert diagnostic.longterm is not None
            assert 'AQUA_region' in diagnostic.longterm.attrs
            assert diagnostic.longterm.attrs['AQUA_region'] == 'Tropics'
    
    @pytest.mark.parametrize("freq,with_std", [
        ('seasonal', False),
        ('seasonal', True),
        ('longterm', True)
    ])
    def test_save_with_region(self, tmp_path, freq, with_std):
        """Test saving data with region information"""
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
        diagnostic.compute_dim_mean(freq=freq)
        
        if with_std:
            diagnostic.compute_std(freq=freq)
        
        diagnostic.save_netcdf(freq=freq, outputdir=str(tmp_path), rebuild=True)
        self._assert_files_created(tmp_path)


@pytest.mark.diagnostics
class TestLatLonProfilesErrors:
    """Test error handling in LatLonProfiles class"""
    
    def test_invalid_mean_type_in_compute(self):
        """Test that invalid mean_type raises error in compute_dim_mean"""
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
        
        with pytest.raises(ValueError, match='Mean type invalid not recognized for std computation'):
            diagnostic.compute_std(freq='seasonal')
    
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
        
        # No files should be created when data is missing
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) == 0, "No files should be created when data is missing"


@pytest.mark.diagnostics
class TestLatLonProfilesRealization:
    """Test realization extraction from data attributes"""
    
    def test_realization_in_filenames(self, tmp_path):
        """Test that realization appears in saved filenames"""
        diagnostic = LatLonProfiles(
            model='IFS',
            exp='test-tco79',
            source='teleconnections',
            startdate='1991-01-01',
            enddate='1992-12-31',
            mean_type='zonal',
            loglevel=loglevel
        )
        
        diagnostic.retrieve(var='skt')
        
        # Manually set realization to test
        diagnostic.realization = 'r5'
        if hasattr(diagnostic.data, 'attrs'):
            diagnostic.data.attrs['AQUA_realization'] = 'r5'
        
        assert diagnostic.realization == 'r5'
        
        diagnostic.compute_dim_mean(freq='longterm')
        diagnostic.save_netcdf(freq='longterm', outputdir=str(tmp_path), rebuild=True)
        
        files = list(tmp_path.rglob('*.nc'))
        assert len(files) > 0
        assert any('r5' in f.name for f in files), "Realization 'r5' not found in any filename"
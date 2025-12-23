"""Testing if fldmean method works"""

import pytest
from aqua import Reader, FldStat
from conftest import LOGLEVEL

# Aliases with module scope for fixtures
@pytest.fixture(scope='module')
def reader_fesom(fesom_test_pi_original_2d_reader):
    return fesom_test_pi_original_2d_reader

@pytest.fixture(scope='module')
def data_fesom(fesom_test_pi_original_2d_data):
    return fesom_test_pi_original_2d_data

@pytest.fixture(scope='module')
def reader_icon(icon_test_healpix_short_reader):
    return icon_test_healpix_short_reader

@pytest.fixture(scope='module')
def data_icon(icon_test_healpix_short_data):
    return icon_test_healpix_short_data

@pytest.fixture(scope='module')
def reader_icon_r2b0(icon_test_r2b0_short_reader):
    return icon_test_r2b0_short_reader

@pytest.fixture(scope='module')
def data_icon_r2b0(icon_test_r2b0_short_data):
    return icon_test_r2b0_short_data

@pytest.fixture(scope='module')
def reader_nemo_2d(nemo_test_eORCA1_long_2d_reader):
    return nemo_test_eORCA1_long_2d_reader

@pytest.fixture(scope='module')
def data_nemo_2d(nemo_test_eORCA1_long_2d_data):
    return nemo_test_eORCA1_long_2d_data

@pytest.fixture(scope='module')
def reader_nemo_3d(nemo_test_eORCA1_short_3d_reader):
    return nemo_test_eORCA1_short_3d_reader

@pytest.fixture(scope='module')
def data_nemo_3d(nemo_test_eORCA1_short_3d_data):
    return nemo_test_eORCA1_short_3d_data

@pytest.fixture(scope='module')
def reader_ifs(ifs_tco79_short_reader):
    return ifs_tco79_short_reader

@pytest.fixture(scope='module')
def data_ifs(ifs_tco79_short_data_2t):
    return ifs_tco79_short_data_2t

# Test classes
@pytest.mark.aqua
class TestFldModule():
    """Class for fldmean standalone"""

    def test_fldmean_from_data(self):
        """test Fldmean class native from data"""
        reader = Reader(catalog='ci', model='IFS', exp='test-tco79',
                        source='short', regrid='r100', rebuild=True)
        data = reader.retrieve(var='2t')
        fldmodule = FldStat(area=reader.src_grid_area.cell_area, loglevel=LOGLEVEL)
        assert fldmodule.fldstat(data, stat='mean')['2t'].size == 2
        fldmodule = FldStat(loglevel=LOGLEVEL)
        assert fldmodule.fldstat(data, stat='mean')['2t'].size == 2

    def test_fldmean_from_data_selection(self):
        """test Fldmean class native from data with reversed lat"""
        reader = Reader(catalog='ci', model='IFS', exp='test-tco79',
                        source='long', regrid='r100', rebuild=True)
        data = reader.retrieve(var='2t')
        reverted = data.reindex({'lat': list(reversed(data.coords['lat']))})
        reverted = reverted.isel(time=slice(0, 3))
        fldmodule = FldStat(area=reader.src_grid_area.cell_area, loglevel=LOGLEVEL)
        assert fldmodule.fldstat(reverted, stat='mean')['2t'].size == 3

    def test_fldmean_raise(self):
        """test Fldmean class raise error if no area provided"""
        with pytest.raises(ValueError, match="Area must be an xarray DataArray or Dataset."):
            FldStat(loglevel=LOGLEVEL, area='pippo')

@pytest.mark.aqua
class TestFldmean():
    """Test class for fldmean"""

    @pytest.mark.parametrize(('source, value, shape'),
                             [('short', 285.75920, 2),
                             ('short_nn', 285.75920, 2),
                             ('long', 285.86724, 20)])
    def test_fldmean_ifs(self, source, value, shape):
        """Fldmean test for IFS"""
        reader = Reader(model="IFS", exp="test-tco79", source=source, loglevel=LOGLEVEL)
        data = reader.retrieve(var='2t')
        if source == 'long':
            data = data.isel(time=slice(0, 20))
        avg = reader.fldmean(data['2t']).values
        assert avg.shape == (shape,)
        assert avg[1] == pytest.approx(value)

    def test_fldmean_fesom(self, reader_fesom, data_fesom):
        """Fldmean test for FESOM"""
        avg = reader_fesom.fldmean(data_fesom['tos']).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(291.1306)

    def test_fldmean_fesom_selection(self):
        """Fldmean test for FESOM"""
        reader = Reader(model="FESOM", exp="test-pi", source='original_2d',
                        regrid='r100', loglevel=LOGLEVEL)
        data = reader.retrieve(var='tos')
        data = reader.regrid(data)
        avg = reader.fldmean(data['tos'], lon_limits=[50, 90], lat_limits=[10, 40]).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(300.1865)

    def test_fldmean_healpix(self, reader_icon, data_icon):
        """Fldmean test for ICON"""
        avg = reader_icon.fldmean(data_icon['2t']).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(286.1479)

    def test_fldmean_healpix_selection(self):
        """Fldmean test for ICON with area selection"""
        reader = Reader(model="ICON", exp="test-healpix", source='short',
                        regrid='r200', loglevel=LOGLEVEL)
        data = reader.retrieve(var='2t')
        data = reader.regrid(data)
        avg = reader.fldmean(data['2t'],  lon_limits=[-30, 50], lat_limits=[-30, -90])
        assert "Area selection: lat=[-90, -30], lon=[330, 50]" in avg.history
        assert avg.values.shape == (2,)
        assert avg.values[0] == pytest.approx(285.131484)

    def test_fldmean_healpix_selection_lat_only(self):
        """Fldmean test for ICON with area selection, only lat"""
        reader = Reader(model="ICON", exp="test-healpix", source='short',
                        regrid='r200', loglevel=LOGLEVEL)
        data = reader.retrieve(var='2t')
        data = reader.regrid(data)
        avg = reader.fldmean(data['2t'], lat_limits=[-30, 30]).values
        assert avg.shape == (2,)
        assert avg[0] == pytest.approx(292.6823)

    def test_fldmean_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Fldmean test for ICON"""
        avg = reader_icon_r2b0.fldmean(data_icon_r2b0['t']).values
        assert avg.shape == (2, 90)
        assert avg[1, 1] == pytest.approx(214.4841)

    def test_fldmean_regridded(self):
        """Fldmean test for regridded data"""
        reader = Reader(model='FESOM', exp='test-pi', source='original_2d',
                        regrid='r250', loglevel=LOGLEVEL)
        data = reader.retrieve(var='tos')
        avg = reader.fldmean(data['tos']).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(291.1306)

    def test_fldmean_nemo(self, reader_nemo_2d, data_nemo_2d):
        """Fldmean test for NEMO"""
        avg = reader_nemo_2d.fldmean(data_nemo_2d['tos']).values
        assert avg.shape == (6,)
        assert avg[5] == pytest.approx(290.5516)

    def test_fldmean_nemo_3d(self, reader_nemo_3d, data_nemo_3d):
        """Fldmean test for NEMO-3D"""
        avg = reader_nemo_3d.fldmean(data_nemo_3d['so']).values
        assert avg.shape == (8,)
        assert avg[4] == pytest.approx(34.63406)

    def test_fldmean_era5_toward_gaussian(self):
        """Fldmean test for ERA5 toward gaussian grid and verification of fldmean"""
        reader = Reader(model='ERA5', exp='era5-hpz3', source='monthly',
                        regrid='F32', loglevel=LOGLEVEL)
        data = reader.retrieve(var='2t').isel(time=0)
        regrid = reader.regrid(data)
        avg_reg = reader.fldmean(regrid['2t'])
        assert regrid.lat[0] > regrid.lat[1]  # verify lat decreasing
        assert avg_reg.values == pytest.approx(285.426661) # verify fldmean value

@pytest.mark.aqua
class TestFldStatDims():
    """Test class for dims parameter functionality"""

    def test_fldmean_custom_dims_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Test fldmean with custom dims parameter on ICON grid"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)
        
        # Test with explicit horizontal dims (should be ['cell'] for ICON)
        result = fldmodule.fldstat(data_icon_r2b0['t'], stat='mean', dims=['cell'])
        assert result.shape == (2, 90)  # time, height levels
        assert result.values[1, 1] == pytest.approx(214.4841)  # Same value as existing test

    def test_fldmean_partial_dims_icon_3d(self, reader_icon_r2b0, data_icon_r2b0):
        """Test fldmean with subset of horizontal dims on 3D data"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)
        
        # Test averaging over only spatial dimension, keeping height
        result = fldmodule.fldstat(data_icon_r2b0['t'], stat='mean', dims=['cell'])
        # Should preserve height dimension but average over space
        assert 'height' in result.dims
        assert 'cell' not in result.dims
        assert result.shape == (2, 90)

    def test_fldmean_dims_validation_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Test dims validation with ICON data"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)
        
        # Test invalid dimension
        with pytest.raises(ValueError, match="Dimension invalid_dim not found in horizontal dimensions"):
            fldmodule.fldstat(data_icon_r2b0['t'], stat='mean', dims=['invalid_dim'])

    def test_fldmean_dims_default_vs_explicit_icon(self, reader_icon_r2b0, data_icon_r2b0):
        """Test that default and explicit dims give same results on ICON"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)
        
        # Compare default behavior with explicit dims
        result_default = fldmodule.fldstat(data_icon_r2b0['t'], stat='mean')
        result_explicit = fldmodule.fldstat(data_icon_r2b0['t'], stat='mean', dims=['cell'])
        
        # Results should be identical
        assert result_default.equals(result_explicit)
        assert result_default.values[1, 1] == pytest.approx(214.4841)

    def test_fldmean_dims_not_list(self, reader_icon_r2b0, data_icon_r2b0):
        """Test that dims must be a list"""
        fldmodule = FldStat(area=reader_icon_r2b0.src_grid_area.cell_area, loglevel=LOGLEVEL)
        
        with pytest.raises(ValueError, match="dims must be a list of dimension names."):
            fldmodule.fldstat(data_icon_r2b0['t'], stat='mean', dims='cell')

@pytest.mark.aqua
class TestFldStatWrappers():
    """Test class for fldstat wrapper methods"""
    
    def test_fldmean(self, reader_ifs, data_ifs):
        """Test fldmean wrapper method"""
        avg = reader_ifs.fldmean(data_ifs['2t'])
        assert avg[1] == pytest.approx(285.75920)
        avg_st = reader_ifs.fldstat(data_ifs['2t'], stat='mean')
        assert avg[1] == pytest.approx(285.75920)
        assert avg_st[1] == pytest.approx(285.75920)

    def test_fldmax(self, reader_ifs, data_ifs):
        """Test fldmax wrapper method"""
        maxval = reader_ifs.fldmax(data_ifs['2t'])
        avg = reader_ifs.fldmean(data_ifs['2t'])
        assert maxval[1].values == pytest.approx(310.6103)
        # max should be greater than or equal to mean
        assert maxval[0] >= avg[0]

    def test_fldmin(self, reader_ifs, data_ifs  ):
        """Test fldmin wrapper method"""
        minval = reader_ifs.fldmin(data_ifs['2t'])
        assert minval[1].values == pytest.approx(232.79393)
        maxval = reader_ifs.fldmax(data_ifs['2t'])
        assert minval[0] <= maxval[0]

    def test_fldstd(self, reader_ifs, data_ifs):
        """Test fldstd wrapper method"""
        stdval = reader_ifs.fldstd(data_ifs['2t'])
        assert stdval[1].values == pytest.approx(15.6998)

    def test_fldsum(self, reader_ifs, data_ifs):
        """Test fldsum wrapper method"""
        sumval = reader_ifs.fldsum(data_ifs['2t'])
        assert sumval[1].values == pytest.approx(1.4572956e+17)

    def test_fldintg(self, reader_ifs, data_ifs):
        """Test fldintg wrapper method"""
        intgval = reader_ifs.fldintg(data_ifs['2t'])
        assert intgval[0].values == pytest.approx(1.4583612e+17)

    def test_fldarea(self, reader_ifs, data_ifs):
        """Test fldsum wrapper method"""
        asumval = reader_ifs.fldarea(data_ifs['2t'])
        assert asumval[0].values == pytest.approx(5.0997329e+14)

    def test_fldstat_compare(self, reader_ifs, data_ifs):
        """Test that wrapper methods give consistent results"""
        data_var = data_ifs['2t']
        
        # Compare wrapper methods with generic fldstat
        mean_wrapper = reader_ifs.fldmean(data_var)
        mean_generic = reader_ifs.fldstat(data_var, stat='mean')
        
        max_wrapper = reader_ifs.fldmax(data_var)
        max_generic = reader_ifs.fldstat(data_var, stat='max')
        
        assert mean_wrapper.equals(mean_generic)
        assert max_wrapper.equals(max_generic)
        
        # Test logical relationships
        assert (reader_ifs.fldmin(data_var) <= reader_ifs.fldmean(data_var)).all()
        assert (reader_ifs.fldmean(data_var) <= reader_ifs.fldmax(data_var)).all()
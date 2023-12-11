"""Testing if fldmean method works"""

import pytest
from aqua import Reader

loglevel = "DEBUG"


@pytest.mark.aqua
class TestFldmean():
    """Test class for fldmean"""

    @pytest.mark.parametrize(('source,value,shape'),
                             [('short', 285.75920, 2),
                             ('short_nn', 285.75920, 2),
                             ('long', 285.86724, 4728)])
    def test_fldmean_ifs(self, source, value, shape):
        """Fldmean test for IFS"""
        reader = Reader(model="IFS", exp="test-tco79", source=source, loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['2t']).values
        assert avg.shape == (shape,)
        # assert avg[0] == pytest.approx(285.96816)
        assert avg[1] == pytest.approx(value)

    def test_fldmean_fesom(self):
        """Fldmean test for FESOM"""
        reader = Reader(model="FESOM", exp="test-pi", source='original_2d', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['sst']).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(291.1306)

    def test_fldmean_fesom_selection(self):
        """Fldmean test for FESOM"""
        reader = Reader(model="FESOM", exp="test-pi", source='original_2d',
                        regrid='r100', loglevel=loglevel)
        data = reader.retrieve()
        data = reader.regrid(data)
        avg = reader.fldmean(data['sst'], lon_limits=[50, 90], lat_limits=[10, 40]).values
        assert avg.shape == (2,)
        # assert avg[1] == pytest.approx(17.9806)
        assert avg[1] == pytest.approx(300.1865)

    def test_fldmean_healpix(self):
        """Fldmean test for FESOM"""
        reader = Reader(model="ICON", exp="test-healpix", source='short', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['2t']).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(286.1479)

    def test_fldmean_healpix_selection(self):
        """Fldmean test for FESOM with area selection"""
        reader = Reader(model="ICON", exp="test-healpix", source='short',
                        regrid='r200', loglevel=loglevel)
        data = reader.retrieve()
        data = reader.regrid(data)
        avg = reader.fldmean(data['2t'],  lon_limits=[-30, 50], lat_limits=[-30, -90]).values
        assert avg.shape == (2,)
        assert avg[0] == pytest.approx(285.131484)

    def test_fldmean_healpix_selection_lat_only(self):
        """Fldmean test for FESOM with area selection, only lat"""
        reader = Reader(model="ICON", exp="test-healpix", source='short',
                        regrid='r200', loglevel=loglevel)
        data = reader.retrieve()
        data = reader.regrid(data)
        avg = reader.fldmean(data['2t'], lat_limits=[-30, 30]).values
        assert avg.shape == (2,)
        assert avg[0] == pytest.approx(292.6823)

    def test_fldmean_icon(self):
        """Fldmean test for ICON"""
        reader = Reader(model="ICON", exp="test-r2b0", source='short', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['t']).values
        assert avg.shape == (2, 90)
        assert avg[1, 1] == pytest.approx(214.4841)

    def test_fldmean_regridded(self):
        """Fldmean test for regridded data"""
        reader = Reader(model='FESOM', exp='test-pi', source='original_2d',
                        regrid='r250', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['sst']).values
        assert avg.shape == (2,)
        assert avg[1] == pytest.approx(291.1306)

    def test_fldmean_nemo(self):
        """Fldmean test for NEMO"""
        reader = Reader(model="NEMO", exp="test-eORCA1", source='long-2d', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['sst']).values
        assert avg.shape == (6,)
        assert avg[5] == pytest.approx(290.5516)

    def test_fldmean_nemo_3d(self):
        """Fldmean test for NEMO-3D"""
        reader = Reader(model="NEMO", exp="test-eORCA1", source='short-3d', loglevel=loglevel)
        data = reader.retrieve()
        avg = reader.fldmean(data['avg_so']).values
        assert avg.shape == (75,)
        assert avg[11] == pytest.approx(34.06011)

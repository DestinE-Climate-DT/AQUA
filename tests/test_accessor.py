"""Testing if the accessor works"""

import pytest
from aqua import Reader

# Aliases with module scope for fixtures
@pytest.fixture(scope='module')
def reader_instance(ifs_tco79_short_r100_reader):
    return ifs_tco79_short_r100_reader

@pytest.fixture(scope='module')
def data(ifs_tco79_short_r100_data):
    return ifs_tco79_short_r100_data

@pytest.fixture(scope='module')
def reader_instance2(ifs_tco79_short_r200_reader):
    return ifs_tco79_short_r200_reader

@pytest.fixture(scope='module')
def data2(ifs_tco79_short_r200_data):
    return ifs_tco79_short_r200_data

# Test classes
@pytest.mark.aqua
class TestAccessor():
    """Test class for accessor"""

    def test_accessor_fldmean(self, data):
        """Test fldmean as accessor"""
        avg = data.aqua.fldmean()['2t'].values
        assert avg[1] == pytest.approx(285.75920)

    def test_accessor_histogram(self, data):
        """Test histogram as accessor"""
        hist = data['2t'].aqua.histogram(bins=200, range=(150, 350), weighted=False)
        assert sum(hist.values) == data['2t'].size

    def test_accessor_two(self, reader_instance, reader_instance2, data, data2):
        """Test the use of two reader instances"""
        # test accessor on dataset, the object should remember the correct reader
        data1r = data.aqua.regrid()
        data2r = data2.aqua.regrid()

        assert len(data1r.lon) == 360
        assert len(data2r.lon) == 180

        # Check if the accessor still works with the result
        reader_instance.set_default()  # Make sure that the correct reader is used
        avg = data1r.aqua.fldmean()['2t'].values
        assert avg[1] == pytest.approx(285.7543, rel=1e-4)

        # Alternative way to do it
        avg = data2r.aqua.set_default(reader_instance2).aqua.fldmean()['2t'].values
        assert avg[1] == pytest.approx(285.7543, rel=1e-4)

        # Test accessor on dataarray (should be using the previous default)
        avgda = data2['2t'].aqua.fldmean().values
        assert avgda[1] == pytest.approx(285.7543, rel=1e-4)
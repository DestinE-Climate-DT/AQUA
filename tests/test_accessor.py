"""Testing if the accessor works"""

import pytest
from aqua import Reader

loglevel = "DEBUG"


@pytest.fixture
def reader_instance():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=loglevel, regrid="r100")

@pytest.fixture
def reader_instance2():
    return Reader(model="IFS", exp="test-tco79", source="short", loglevel=loglevel, regrid="r200")

@pytest.mark.aqua
class TestAccessor():
    """Test class for accessor"""

    def test_accessor_fldmean(self, reader_instance):
        """Test fldmean as accessor"""
        data = reader_instance.retrieve()
        avg = data.aqua.fldmean()['2t'].values
        assert avg[1] == pytest.approx(285.75920)

    def test_accessor_two(self, reader_instance, reader_instance2):
        """Test the use of two reader instances"""
 
        data1 = reader_instance.retrieve()
        data2 = reader_instance2.retrieve()

        # test accessor on dataset, the object should remember the correct reader
        data1r = data1.aqua.regrid()
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
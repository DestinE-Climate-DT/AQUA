"""Testing if fldmean method works"""

import pytest
from aqua import Reader


@pytest.mark.parametrize(('source,value,shape'),
                         [('short', 285.75920, 2), ('long', 285.86724, 4728)])
def test_fldmean_ifs(source, value, shape):
    """Fldmean test for IFS"""
    reader = Reader(model="IFS", exp="test-tco79", source=source)
    data = reader.retrieve()
    avg = reader.fldmean(data['2t']).values
    assert avg.shape == (shape,)
    # assert avg[0] == pytest.approx(285.96816)
    assert avg[1] == pytest.approx(value)


def test_fldmean_fesom():
    """Fldmean test for FESOM"""
    reader = Reader(model="FESOM", exp="test-pi", source='original_2d')
    data = reader.retrieve()
    avg = reader.fldmean(data['sst']).values
    assert avg.shape == (2,)
    assert avg[1] == pytest.approx(17.9806)

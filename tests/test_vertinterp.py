"""Tests for streaming"""

import pytest
from aqua import Reader

loglevel = "DEBUG"

@pytest.mark.aqua
def test_vertinterp():
    """Trivial test for vertical interpolation. to be expanded"""

    reader = Reader(model='FESOM', exp='test-pi', source='original_3d', loglevel=loglevel)
    data = reader.retrieve()
    select = data.isel(time=0)

    # dataarray access
    interp = reader.vertinterp(select['thetao'], levels=10, units='m',
                               vert_coord='nz1')

    assert pytest.approx(interp[40].values) == 272.64060

    # dataset access
    interp = reader.vertinterp(select, levels=10, vert_coord='nz1')
    assert pytest.approx(interp['thetao'][40].values) == 272.64060

    # change unit
    interp = reader.vertinterp(select['thetao'], levels=[0.1, 0.3], units='km', vert_coord='nz1')
    assert interp.shape == (2, 3140)

@pytest.mark.aqua
def test_vertinterp_exceptions():
    """"Test exceptions for vertical interpolation"""

    reader = Reader(model='FESOM', exp='test-pi', source='original_3d', loglevel=loglevel)
    data = reader.retrieve()
    select = data.isel(time=0)

    # wrong vert_coord
    with pytest.raises(KeyError):
        reader.vertinterp(select['ocpt'], levels=10, units='m', vert_coord='nz2')

    # no levels
    with pytest.raises(KeyError):
        reader.vertinterp(select['ocpt'], units='m', vert_coord='nz1')

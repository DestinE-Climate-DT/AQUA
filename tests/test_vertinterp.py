"""Tests for streaming"""

import pytest
from aqua import Reader
from conftest import LOGLEVEL

loglevel = LOGLEVEL

@pytest.fixture(scope='module')
def reader():
    return Reader(model='FESOM', exp='test-pi', source='original_3d', loglevel=loglevel)

@pytest.fixture(scope='module')
def data(reader):
    """Retrieve 3D FESOM data once for all vertinterp tests"""
    return reader.retrieve()
    
@pytest.mark.aqua
def test_vertinterp(reader, data):
    """Trivial test for vertical interpolation. to be expanded"""
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
def test_vertinterp_exceptions(reader, data):
    """"Test exceptions for vertical interpolation"""
    select = data.isel(time=0)

    # wrong vert_coord
    with pytest.raises(KeyError):
        reader.vertinterp(select['ocpt'], levels=10, units='m', vert_coord='nz2')

    # no levels
    with pytest.raises(KeyError):
        reader.vertinterp(select['ocpt'], units='m', vert_coord='nz1')

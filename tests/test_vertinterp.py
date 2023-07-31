"""Tests for streaming"""

import pytest
from aqua import Reader

@pytest.mark.aqua
def test_vertinterp():
    """Trivial test for vertical interpolation. to be expanded"""

    reader = Reader(model='FESOM', exp='test-pi', source='original_3d')
    data = reader.retrieve()
    select = data.isel(time=0)

    # dataarray access
    interp = reader.vertinterp(select['ocpt'], levels = 10, units = 'm', vert_coord='nz1')
    assert pytest.approx(interp[40:41].values[0]) == -1.09438590

    # dataset access
    interp = reader.vertinterp(select, levels = 10, vert_coord='nz1')
    assert pytest.approx(interp['ocpt'][40:41].values[0]) == -1.09438590

    # change unit
    interp = reader.vertinterp(select['ocpt'], levels = [0.1, 0.2, 0.3], units = 'km', vert_coord='nz1')
    assert interp.shape == (3, 3140)
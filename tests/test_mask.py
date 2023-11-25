"""Test masked source"""

import pytest
import numpy as np
from aqua import Reader

loglevel = "DEBUG"
approx_rel = 1e-4

@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short_masked", "2t", "r100")
    ]
)
def reader_arguments(request):
    return request.param

@pytest.mark.aqua
class TestMask():
    """class for masked test"""

    def test_is_masked(self, reader_arguments):
        """
        Test if the masked source is correctly read
        """
        model, exp, source, variable, regrid = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, regrid=regrid,
                        fix=True, loglevel=loglevel)
        data = reader.retrieve()
        rgd = reader.regrid(data[variable])
        masked = rgd.isel(time=0).values

        assert not np.isnan(masked[179, 288])
        assert masked[179, 288] == pytest.approx(246.2005, rel=approx_rel)
        assert np.isnan(masked[169, 18])

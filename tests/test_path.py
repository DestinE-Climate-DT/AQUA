"""Test path handling"""

import pytest
from conftest import APPROX_REL, LOGLEVEL

from aqua import Reader


@pytest.mark.aqua
class TestRegridderPath:
    """class for regridding test"""

    def test_regridder_path(self):
        """Test the regridder with a path"""

        reader = Reader(
            path="./AQUA_tests/models/IFS/long/regridded_r18x9.nc", regrid="r40x20", loglevel=LOGLEVEL, rebuild=True
        )
        data = reader.retrieve()
        sample = data["2t"].isel(time=0)
        rgd = reader.regrid(sample)

        assert len(rgd.lon) == 40
        assert len(rgd.lat) == 20

        assert reader.fldmean(rgd).values == pytest.approx(285.98483, rel=APPROX_REL)
        assert reader.fldmax(sample).values == pytest.approx(301.3355, rel=APPROX_REL)

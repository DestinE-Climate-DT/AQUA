"""Test path handling"""

import pytest
from utils_tests import APPROX_REL, LOGLEVEL

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

    def test_labels_with_path(self):
        """model/exp/source/catalog given with path are stored as AQUA_* attributes, not ignored."""
        reader = Reader(
            path="./AQUA_tests/models/IFS/long/regridded_r18x9.nc",
            model="IFS",
            exp="long",
            source="regridded",
            catalog="ci",
            areas=False,
            loglevel=LOGLEVEL,
        )
        attrs = reader.retrieve()["2t"].attrs
        assert {k: attrs[f"AQUA_{k}"] for k in ("catalog", "model", "exp", "source")} == {
            "catalog": "ci",
            "model": "IFS",
            "exp": "long",
            "source": "regridded",
        }

    def test_missing_triplet(self):
        """Without path or url, the intake triplet must be complete."""
        with pytest.raises(ValueError, match=r"Missing \['source'\]"):
            Reader(model="IFS", exp="test-tco79", areas=False)

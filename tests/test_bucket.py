import pytest
from conftest import APPROX_REL, LOGLEVEL

from aqua import Reader


@pytest.mark.aqua
class TestBucket:
    """Test Reader with bucket functionality"""

    def test_bucket_lumi(self):
        """
        Test the bucket functionality with LUMI data
        """
        var = "2t"
        reader = Reader(
            catalog="climatedt-gen2",
            model="IFS-NEMO-5km",
            exp="baseline-hist",
            source="lra-r100-monthly-zarr",
            areas=False,
            loglevel=LOGLEVEL,
        )
        data = reader.retrieve(var=var)

        assert data is not None
        assert data[var].isel(time=0).mean().values == pytest.approx(276.83795561, rel=APPROX_REL)

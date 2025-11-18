import pytest
from aqua import Reader
import pandas as pd
from conftest import APPROX_REL, LOGLEVEL

approx_rel = APPROX_REL
loglevel = LOGLEVEL

@pytest.fixture
def reader_instance():
    return Reader(model="IFS", exp="test-tco79", source="intake-esm-test",
                  areas=False, fix=False, loglevel=loglevel)

# aqua class for tests
@pytest.mark.aqua
class TestAqua:
    """ESM tests for AQUA reader"""

    def test_retrieve_esm_shape(self, reader_instance):
        """
        Test if the retrieve method returns data with the expected shape
        """
        data = reader_instance.retrieve()
        assert len(data) > 0
        assert data['2t'].shape == (2, 28480)

    def test_retrieve_esm_value(self, reader_instance):
        """
        Test if the retrieve method returns data with the expected average value
        """
        data = reader_instance.retrieve()
        assert data["2t"].mean().values == pytest.approx(286.48692342, rel=approx_rel)

    def test_retrieve_esm_var(self, reader_instance):
        """
        Test if the retrieve method returns data with the expected average value
        if a variable is specified
        """
        data = reader_instance.retrieve(var='2t')
        assert data["2t"].mean().values == pytest.approx(286.48692342, rel=approx_rel)

    def test_retrieve_esm_date_slice(self, reader_instance):
        """
        Test if the retrieve method correctly slices data by startdate and enddate
        """
        data = reader_instance.retrieve(startdate='2020-01-20', enddate='2020-01-20')
        assert len(data) > 0
        assert '2t' in data
        assert 'time' in data.dims
        # Verify the time range is correct
        assert data.time.min().dt.date.values == pytest.approx(pd.Timestamp('2020-01-20').date())
        assert data.time.max().dt.date.values == pytest.approx(pd.Timestamp('2020-01-20').date())

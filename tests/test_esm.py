import pytest
from aqua import Reader

# pytest approximation, to bear with different machines
approx_rel = 1e-4

@pytest.fixture(scope='module')
def data(ifs_tco79_intake_esm_data):
    return ifs_tco79_intake_esm_data

# aqua class for tests
@pytest.mark.aqua
class TestAqua:
    """ESM tests for AQUA reader"""

    def test_retrieve_esm_shape(self, data):
        """
        Test if the retrieve method returns data with the expected shape
        """
        assert len(data) > 0
        assert data['2t'].shape == (2, 28480)

    def test_retrieve_esm_value(self, data):
        """
        Test if the retrieve method returns data with the expected average value
        """
        assert data["2t"].mean().values == pytest.approx(286.48692342, rel=approx_rel)

    def test_retrieve_esm_var(self, data):
        """
        Test if the retrieve method returns data with the expected average value
        if a variable is specified
        """
        assert data["2t"].mean().values == pytest.approx(286.48692342, rel=approx_rel)

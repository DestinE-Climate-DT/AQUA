import pytest
from aqua import Reader

@pytest.mark.aqua
class TestKwargs:
    """Small class to test kwargs"""

    def test_kwargs_default(self):
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d")
        data = reader.retrieve()
        assert list(data.data_vars) == ['avg_tos']
        assert data.time.dt.year == 1985

    @pytest.mark.aqua
    def test_kwargs_one_option(self):
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d", year=1986)
        data = reader.retrieve()
        assert list(data.data_vars) == ['avg_tos']
        assert data.time.dt.year == 1986

    @pytest.mark.aqua
    def test_kwargs_two_options(self):
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d", year=1986, variable='a_ice', fix=False)
        data = reader.retrieve()
        assert list(data.data_vars) == ['a_ice']
        assert data.time.dt.year == 1986

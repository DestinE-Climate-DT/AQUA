import pytest
from utils_tests import LOGLEVEL

from aqua import Reader


@pytest.mark.aqua
class TestKwargs:
    """Small class to test kwargs"""

    @pytest.mark.parametrize(
        "kwargs, variable, year",
        [
            pytest.param({}, "tos", 1985, id="default"),
            pytest.param({"year": 1986}, "tos", 1986, id="one-option"),
            pytest.param({"year": 1986, "variable": "a_ice", "fix": False}, "a_ice", 1986, id="two-options"),
            # With a wrong kwarg, the reader will remove it and log a warning
            pytest.param({"wrong_kwarg": "pippo"}, "tos", 1985, id="wrong-kwarg"),
        ],
    )
    def test_kwargs(self, kwargs, variable, year):
        """The source kwargs select the variable and the year; unknown ones are dropped."""
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d", loglevel=LOGLEVEL, **kwargs)
        data = reader.retrieve()
        assert list(data.data_vars) == [variable]
        assert data.time.dt.year == year

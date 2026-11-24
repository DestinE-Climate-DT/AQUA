import pytest
from intake.catalog.local import LocalCatalogEntry
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

    def test_metadata_templated_on_kwargs(self, monkeypatch):
        """Metadata templated on an intake parameter follows the value passed to the Reader."""
        init = LocalCatalogEntry.__init__

        def templated_init(entry, name, *args, metadata=None, **kwargs):
            if name == "kwargs-2d":  # as the online HEALPix sources do with hpz{{ zoom }}-nested
                metadata = {**metadata, "source_grid_name": "test-pi-{{ variable }}"}
            init(entry, name, *args, metadata=metadata, **kwargs)

        monkeypatch.setattr(LocalCatalogEntry, "__init__", templated_init)
        reader = Reader(model="FESOM", exp="test-pi", source="kwargs-2d", variable="a_ice", areas=False, loglevel=LOGLEVEL)
        assert reader.src_grid_name == "test-pi-a_ice"

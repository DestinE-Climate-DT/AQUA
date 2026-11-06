"""Test levels selection in Reader.retrieve for a netcdf source"""

import pytest


@pytest.fixture(scope="module")
def reader_era5(era5_hpz3_monthly_reader):
    """Fixture to provide the ERA5 reader instance, using the shared session fixture."""
    return era5_hpz3_monthly_reader


@pytest.mark.aqua
class TestLevels:
    """Tests for the level selection in the retrieve method."""

    @pytest.mark.parametrize(
        "level, expected",
        [
            pytest.param(50000, [50000], id="single-int"),
            pytest.param(50000.0, [50000.0], id="single-float"),
            pytest.param([50000, 70000], [50000, 70000], id="list-int"),
            pytest.param([50000.0, 70000.0], [50000.0, 70000.0], id="list-float"),
            pytest.param([50000, 70000.0, 85000], [50000, 70000, 85000], id="list-mixed"),
        ],
    )
    def test_select_levels(self, reader_era5, level, expected):
        """Selecting one level or a list of levels, as int, float or mixed, from a 3D variable keeps exactly those levels."""
        data = reader_era5.retrieve(var="t", level=level)

        assert "plev" in data.coords, "Failed to find vertical coordinate"
        assert sorted(data["plev"].values) == sorted(expected)

    def test_level_not_found_single(self, reader_era5, caplog):
        """Test what happens if a requested level is not found in the data."""
        # When a level is not found, an error is logged and the unfiltered data is returned.
        data = reader_era5.retrieve(var="t", level=80000)

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        # The full data is returned, not filtered
        assert data["plev"].size > 1

        assert "Levels [80000] not found in vertical coordinate plev! Returning unfiltered data!" in caplog.messages

    def test_level_not_found_list(self, reader_era5, caplog):
        """Test what happens if a level in the list is not found."""
        # When a level is not found, an error is logged and the unfiltered data is returned.
        data = reader_era5.retrieve(var="t", level=[50000, 80000], level_coord="plev")

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        # The full data is returned, not filtered, so all original levels should be present.
        # ERA5 't' has 37 levels by default in the monthly-nn source.
        assert data["plev"].size > 2

        assert "Levels [80000] not found in vertical coordinate plev! Returning unfiltered data!" in caplog.messages

    def test_no_levels(self, reader_era5, caplog):
        """Test what happens if the variable has no levels (2D variable)."""
        data = reader_era5.retrieve(var="2t", level=50000)

        assert "plev" not in data.coords, "2D variable should not have a vertical coordinate"

        assert (
            "Levels selected but no level coordinate found in data! "
            "Try specifying the coordinate with the 'level_coord' argument."
        ) in caplog.messages

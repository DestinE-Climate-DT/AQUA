"""Test levels selection in Reader.retrieve for a netcdf source"""

from unittest.mock import MagicMock

import pytest


@pytest.fixture(scope="module")
def reader_era5(era5_hpz3_monthly_reader):
    """Fixture to provide the ERA5 reader instance, using the shared session fixture."""
    return era5_hpz3_monthly_reader


@pytest.mark.aqua
class TestLevels:
    """Tests for the level selection in the retrieve method."""

    def test_single_level_int(self, reader_era5):
        """Test selecting a single level from a 3D variable using integer."""
        data = reader_era5.retrieve(var="t", level=50000)

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        assert data["plev"].size == 1
        assert data["plev"].values[0] == 50000

    def test_single_level_float(self, reader_era5):
        """Test selecting a single level from a 3D variable using float."""
        data = reader_era5.retrieve(var="t", level=50000.0)

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        assert data["plev"].size == 1
        assert data["plev"].values[0] == 50000.0

    def test_list_of_levels_int(self, reader_era5):
        """Test selecting a list of integer levels from a 3D variable."""
        data = reader_era5.retrieve(var="t", level=[50000, 70000])

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        assert data["plev"].size == 2
        assert 50000 in data["plev"].values
        assert 70000 in data["plev"].values

    def test_list_of_levels_float(self, reader_era5):
        """Test selecting a list of float levels from a 3D variable."""
        data = reader_era5.retrieve(var="t", level=[50000.0, 70000.0])

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        assert data["plev"].size == 2
        assert 50000.0 in data["plev"].values
        assert 70000.0 in data["plev"].values

    def test_list_of_levels_mixed_types(self, reader_era5):
        """Test selecting a list with mixed int and float levels from a 3D variable."""
        data = reader_era5.retrieve(var="t", level=[50000, 70000.0, 85000])

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        assert data["plev"].size == 3
        assert 50000 in data["plev"].values or 50000.0 in data["plev"].values
        assert 70000.0 in data["plev"].values or 70000 in data["plev"].values
        assert 85000 in data["plev"].values or 85000.0 in data["plev"].values

    def test_level_not_found_single(self, reader_era5):
        """Test what happens if a requested level is not found in the data."""
        # Mock the logger
        original_logger = reader_era5.backend.logger
        reader_era5.backend.logger = MagicMock()

        # When a level is not found, an error is logged and the unfiltered data is returned.
        data = reader_era5.retrieve(var="t", level=80000)

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        # The full data is returned, not filtered
        assert data["plev"].size > 1

        # Verify logger.error was called
        reader_era5.backend.logger.error.assert_any_call("Levels %s not found in vertical coordinate %s!", [80000], "plev")

        # Restore logger
        reader_era5.backend.logger = original_logger

    def test_level_not_found_list(self, reader_era5):
        """Test what happens if a level in the list is not found."""
        # Mock the logger
        original_logger = reader_era5.backend.logger
        reader_era5.backend.logger = MagicMock()

        # When a level is not found, an error is logged and the unfiltered data is returned.
        data = reader_era5.retrieve(var="t", level=[50000, 80000])

        assert "plev" in data.coords, "Failed to find vertical coordinate"

        # The full data is returned, not filtered, so all original levels should be present.
        # ERA5 't' has 37 levels by default in the monthly-nn source.
        assert data["plev"].size > 2

        # Verify logger.error was called
        reader_era5.backend.logger.error.assert_any_call(
            "Levels %s not found in vertical coordinate %s!", [50000, 80000], "plev"
        )

        # Restore logger
        reader_era5.backend.logger = original_logger

    def test_no_levels(self, reader_era5):
        """Test what happens if the variable has no levels (2D variable)."""
        # Mock the logger
        original_logger = reader_era5.backend.logger
        reader_era5.backend.logger = MagicMock()

        data = reader_era5.retrieve(var="2t", level=50000)

        assert "plev" not in data.coords, "2D variable should not have a vertical coordinate"

        # Verify logger.error was called
        reader_era5.backend.logger.error.assert_any_call("Levels selected but no vertical coordinate found in data!")

        # Restore logger
        reader_era5.backend.logger = original_logger

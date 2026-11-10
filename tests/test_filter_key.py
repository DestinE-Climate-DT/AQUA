import pytest
from utils_tests import LOGLEVEL

from aqua import Reader
from aqua.core.exceptions import NoDataError

pytestmark = pytest.mark.aqua


class TestFilterKey:
    """Tests for the filter_key functionality in AQUA Reader"""

    def test_filter_key_year(self):
        """startdate/enddate keep only the file of 1985 (one yearly mean): fails if the files are not filtered."""
        reader = Reader(
            model="FESOM",
            exp="test-pi",
            source="original_2d_filter",
            startdate="1985-01-01",
            enddate="1985-12-31",
            loglevel=LOGLEVEL,
        )
        data = reader.retrieve()
        assert data.time.dt.year.values.tolist() == [1985]

    def test_filter_key_year_no_dates(self):
        """Dates outside the files leave no file to read"""
        with pytest.raises(NoDataError, match="No files found after filtering the catalog!"):
            _ = Reader(
                model="FESOM",
                exp="test-pi",
                source="original_2d_filter",
                loglevel=LOGLEVEL,
                startdate="2020-01-01",
                enddate="2020-12-31",
            )

    def test_no_filter_key(self):
        """Without dates all the files (one per year) are read"""
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d_filter", loglevel=LOGLEVEL)
        data = reader.retrieve()
        assert data.time.dt.year.values.tolist() == [1985, 1986]

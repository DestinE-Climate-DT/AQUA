import pytest
import numpy as np
from aqua import Reader
from aqua.core.exceptions import NoDataError
from conftest import LOGLEVEL


@pytest.mark.aqua
class TestFilterKey:
    """Tests for the filter_key functionality in AQUA Reader"""
    def test_filter_key_year(self):
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d_filter", 
                        startdate="1985-01-01", enddate="1985-12-31", LOGLEVEL=LOGLEVEL)
        data = reader.retrieve()
        assert data.time.size == 1  # Expecting 12 months of data for the year 1985
        assert np.all(data.time.dt.year == 1985)

    def test_filter_key_year_no_dates(self):
        with pytest.raises(NoDataError, match="No files found after filtering the catalog!"):
            _ = Reader(model="FESOM", exp="test-pi", source="original_2d_filter", 
                        LOGLEVEL=LOGLEVEL, startdate="2020-01-01", enddate="2020-12-31")

    def test_no_filter_key(self):
        reader = Reader(model="FESOM", exp="test-pi", source="original_2d_filter", 
                        LOGLEVEL=LOGLEVEL)
        data = reader.retrieve()
        assert data.time.size == 2  # Expecting multiple years of data
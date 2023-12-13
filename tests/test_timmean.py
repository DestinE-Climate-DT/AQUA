"""Test for timmean method"""

import pytest
import numpy as np
from aqua import Reader

loglevel = "DEBUG"

@pytest.mark.aqua
class TestTimmean():

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmean_monthly(self, var):
        """Timmean test for monthly aggregation"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False, loglevel=loglevel)
        data = reader.retrieve()

        avg = reader.timmean(data[var], freq='monthly')
        nmonths = len(np.unique(data.time.dt.month))
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (nmonths, 9, 18)
        assert len(unique) == nmonths
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmean_monthly_exclude_incomplete(self, var):
        """Timmean test for monthly aggregation with excluded incomplete chunks"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False)
        data = reader.retrieve()
        avg = reader.timmean(data[var], freq='monthly', exclude_incomplete=True)
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (6, 9, 18)
        assert len(unique) == 6
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmean_daily(self, var):
        """Timmean test for daily aggregation"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False, loglevel=loglevel)
        data = reader.retrieve()

        avg = reader.timmean(data[var], freq='daily')
        unique, counts = np.unique(avg.time.dt.day, return_counts=True)
        assert avg.shape == (197, 9, 18)
        assert len(unique) == 31
        assert all(counts == np.array([7, 7, 7, 6, 6, 6, 6, 6, 6, 6,
                                       6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7,
                                       7, 7, 7, 7, 7, 7, 7, 6, 4]))

    def test_timmean_yearly_exclude_incomplete(self):
        """Timmean test for yearly aggregation"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False)
        data = reader.retrieve()
        avg = reader.timmean(data, freq='yearly', exclude_incomplete=True)
        assert avg['ttr'].shape == (0, 9, 18)
        #with pytest.raises(ValueError, match=r'Cannot compute average on .* period, not enough data'):
        #    reader.timmean(data['ttr'], exclude_incomplete=True)

    def test_timmean_pandas(self):
        """Timmean test for weekly aggregation based on pandas labels"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        loglevel=loglevel)

        data = reader.retrieve(var='2t')
        avg = reader.timmean(data, freq='W-MON')

        assert avg['2t'].shape == (29, 9, 18)

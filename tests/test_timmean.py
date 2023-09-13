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
                        freq='monthly', fix=False, loglevel=loglevel)
        data = reader.retrieve()

        avg = reader.timmean(data[var])
        nmonths = len(np.unique(data.time.dt.month))
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (nmonths, 9, 18)
        assert len(unique) == nmonths
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmean_monthly_exclude_incomplete(self, var):
        """Timmean test for monthly aggregation with excluded incomplete chunks"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='monthly', fix=False)
        data = reader.retrieve()
        avg = reader.timmean(data[var], exclude_incomplete=True)
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (6, 9, 18)
        assert len(unique) == 6
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmean_daily(self, var):
        """Timmean test for daily aggregation"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='daily', fix=False, loglevel=loglevel)
        data = reader.retrieve()

        avg = reader.timmean(data[var])
        unique, counts = np.unique(avg.time.dt.day, return_counts=True)
        assert avg.shape == (197, 9, 18)
        assert len(unique) == 31
        assert all(counts == np.array([7, 7, 7, 6, 6, 6, 6, 6, 6, 6,
                                       6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7,
                                       7, 7, 7, 7, 7, 7, 7, 6, 4]))

    def test_timmean_monthly_reader(self):
        """Timmean test for monthly aggregation from Reader directly"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='monthly', fix=False, loglevel=loglevel)
        data = reader.retrieve(timmean=True)

        nmonths = len(np.unique(data.time.dt.month))
        unique, counts = np.unique(data['2t'].time.dt.month,
                                   return_counts=True)
        assert data['2t'].shape == (nmonths, 9, 18)
        assert len(unique) == nmonths
        assert all(counts == counts[0])
    
    def test_timmean_yearly_reader_exclude_incomplete(self):
        """Timmean test for yearly aggregation from Reader directly"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='yearly', exclude_incomplete=True, fix=False)
        data = reader.retrieve(timmean=True)
        assert data['ttr'].shape == (0, 9, 18)
        #with pytest.raises(ValueError, match=r'Cannot compute average on .* period, not enough data'):
        #    reader.timmean(data['ttr'], exclude_incomplete=True)

    def test_timmean_yearly_reader(self):
        """Timmean test for yearly aggregation from Reader directly"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='yearly', fix=False, loglevel=loglevel)
        data = reader.retrieve(timmean=True)

        assert data['ttr'].shape == (1, 9, 18)

    def test_timmean_pandas_reader(self):
        """Timmean test for weekly aggregation based on pandas labels from Reader directly"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        freq='W-MON', loglevel=loglevel)

        data = reader.retrieve(var='2t', timmean=True)
        assert data['2t'].shape == (29, 9, 18)

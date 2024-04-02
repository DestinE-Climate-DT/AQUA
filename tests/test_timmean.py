"""Test for timmean method"""

import pytest
import numpy as np
from aqua import Reader
import pytest

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
        """Timmean test for yearly aggregation with excluded incomplete chunks"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False, loglevel=loglevel)
        data = reader.retrieve(var='ttr')
        avg = reader.timmean(data, freq='yearly', exclude_incomplete=True)
        assert avg['ttr'].shape == (0, 9, 18)
        #with pytest.raises(ValueError, match=r'Cannot compute average on .* period, not enough data'):
        #    reader.timmean(data['ttr'], exclude_incomplete=True)

    def test_timmean_yearly_center_time(self):
        """Timmean test for yearly aggregation with center_time=True"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False, loglevel=loglevel)
        data = reader.retrieve(var='ttr')
        avg = reader.timmean(data, freq='yearly', center_time=True)
        assert avg['ttr'].shape == (1, 9, 18) # 1 year was computed
        assert avg['ttr'].time[0].values == np.datetime64('2020-07-01T00:00:00.000000000')

    def test_timmean_monthly_center_time(self):
        """Timmean test for monthly aggregation with center_time=True"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        fix=False, loglevel=loglevel)
        data = reader.retrieve(var='2t')
        avg = reader.timmean(data, freq='monthly', center_time=True)
        assert avg['2t'].shape == (8, 9, 18)
        assert avg['2t'].time[1].values == np.datetime64('2020-02-15T00:00:00.000000000')

    def test_timmean_daily_center_time(self):
        """Timmean test for daily aggregation with center_time=True and exclude_incomplete=True"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        loglevel=loglevel)
        data = reader.retrieve(var='2t')
        avg = reader.timmean(data, freq='daily', center_time=True, exclude_incomplete=True)
        assert avg['2t'].shape == (197, 9, 18)
        assert avg['2t'].time[1].values == np.datetime64('2020-01-21T12:00:00.000000000')
        
    def test_timmean_pandas_accessor(self):
        """Timmean test for weekly aggregation based on pandas labels"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        loglevel=loglevel)
        data = reader.retrieve(var='2t')
        avg = data.aqua.timmean(freq='W-MON')
        assert avg['2t'].shape == (29, 9, 18)

    def test_timmean_time_bounds(self):
        """Test for _timmean method with time_bounds=True"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        loglevel=loglevel)
        data = reader.retrieve(var='2t')
        avg = reader.timmean(data, freq='monthly', time_bounds=True)
        assert 'time_bnds' in avg
        assert avg['2t'].shape == (8, 9, 18)
        assert avg['time_bnds'].shape == (avg['2t'].shape[0], 2)
        assert np.all(avg['time_bnds'].isel(bnds=0) <= avg['time_bnds'].isel(bnds=1))
    
    def test_timmean_invalid_frequency(self):
        """Test for _timmean method with an invalid frequency"""
        reader = Reader(model="IFS", exp="test-tco79", source='long',
                        loglevel=loglevel)
        data = reader.retrieve(var='2t')
        with pytest.raises(ValueError, match=r'Cant find a frequency to resample, aborting!'):
            reader.timmean(data, freq='invalid')

    def test_timmean_single_timestep(self):
        """Test for _timmean method with a single timestep"""
        reader = Reader(model="IFS", exp="test-tco79", source='long')
        data = reader.retrieve(var='2t')
        with pytest.warns(UserWarning, match=r'A single timestep is available, is this correct?'):
            avg = reader.timmean(data, freq='monthly')
        assert avg.shape == (1, 9, 18)

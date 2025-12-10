"""Test for timmean method"""
import pytest
import numpy as np
from aqua.core.histogram import histogram

@pytest.fixture(scope='module')
def reader(ifs_tco79_long_fixFalse_reader):
    return ifs_tco79_long_fixFalse_reader

@pytest.fixture(scope='module')
def data(ifs_tco79_long_fixFalse_data):
    return ifs_tco79_long_fixFalse_data

@pytest.fixture(scope="module")
def data_2t(reader):
    """Retrieve only 2t variable (most commonly used)"""
    return reader.retrieve(var='2t')

@pytest.fixture(scope="module")
def data_ttr(reader):
    """Retrieve only ttr variable"""
    return reader.retrieve(var='ttr')

@pytest.mark.aqua
class TestTimmean():

    def test_timsum(self, reader, data):
        """Timmean test for sum operation"""
        summed = reader.timsum(data['2t'].isel(lon=0, lat=0), freq='3h')
        assert summed.shape == (1576,)
        assert summed[0] == data['2t'].isel(lon=0, lat=0, time=slice(0, 3)).sum()
        assert np.all(np.unique(summed.time.dt.hour) == np.arange(0, 24, 3))

        with pytest.raises(KeyError, match=r'hypertangent is not a statistic supported by AQUA'):
            reader.timstat(data['2t'], stat='hypertangent', freq='monthly', exclude_incomplete=True)

    @pytest.mark.parametrize('var', ['ttr'])
    def test_timmean_monthly(self, reader, data, var):
        """Timmean test for monthly aggregation"""
        avg = reader.timmean(data[var], freq='monthly')
        nmonths = len(np.unique(data.time.dt.month))
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (nmonths, 9, 18)
        assert len(unique) == nmonths
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t'])
    def test_timstd_allperiod(self, reader, data, var):
        """Timstd test for entire data period"""
        avg = reader.timstd(data[var])
        assert avg.shape == (9, 18)

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timstat_monthly_exclude_incomplete(self, reader, data, var):
        """Timmean test for monthly aggregation with excluded incomplete chunks"""
        avg = reader.timstat(data[var], stat='mean', freq='monthly', exclude_incomplete=True)
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (6, 9, 18)
        assert len(unique) == 6
        assert all(counts == counts[0])

    @pytest.mark.parametrize('var', ['2t', 'ttr'])
    def test_timmax_daily(self, reader, data, var):
        """Timmean test for daily aggregation"""
        avg = reader.timmax(data[var], freq='daily')
        unique, counts = np.unique(avg.time.dt.day, return_counts=True)
        assert avg.shape == (197, 9, 18)
        assert len(unique) == 31
        assert all(counts == np.array([7, 7, 7, 6, 6, 6, 6, 6, 6, 6,
                                       6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7,
                                       7, 7, 7, 7, 7, 7, 7, 6, 4]))
        
    def test_timstat_compare(self, reader, data_2t):
        """Time operations provide robust values"""
        minval = reader.timmin(data_2t['2t'], freq='daily')
        maxval = reader.timmax(data_2t['2t'], freq='daily')
        avg = reader.timmean(data_2t['2t'], freq='daily')
        
        assert (minval <= avg).all()
        assert (avg <= maxval).all()
    def test_timmin_yearly_exclude_incomplete(self, reader, data_ttr):
        """Timmean test for yearly aggregation with excluded incomplete chunks"""
        avg = reader.timmin(data_ttr, freq='yearly', exclude_incomplete=True)
        assert avg['ttr'].shape == (0, 9, 18)

    def test_timmean_yearly_center_time(self, reader, data_ttr):
        """Timmean test for yearly aggregation with center_time=True"""
        avg = reader.timmean(data_ttr, freq='yearly', center_time=True)
        assert avg['ttr'].shape == (1, 9, 18)
        assert avg['ttr'].time[0].values == np.datetime64('2020-07-02T00:00:00.000000000')

    def test_timmean_monthly_center_time(self, reader, data_2t):
        """Timmean test for monthly aggregation with center_time=True"""
        avg = reader.timmean(data_2t, freq='monthly', center_time=True)
        assert avg['2t'].shape == (8, 9, 18)
        assert avg['2t'].time[1].values == np.datetime64('2020-02-15T12:00:00.000000000')

    def test_timstd_daily_center_time(self, reader, data_2t):
        """Timmean test for daily aggregation with center_time=True and exclude_incomplete=True"""
        avg = reader.timstd(data_2t, freq='daily', center_time=True, exclude_incomplete=True)
        assert avg['2t'].shape == (197, 9, 18)
        assert avg['2t'].time[1].values == np.datetime64('2020-01-21T12:00:00.000000000')

    def test_timmean_pandas_accessor(self, reader, data_2t):
        """Timmean test for weekly aggregation based on pandas labels"""
        avg = data_2t.aqua.timmean(freq='W-MON')
        assert avg['2t'].shape == (29, 9, 18)

    def test_timmean_time_bounds(self, reader, data_2t):
        """Test for timmean method with time_bounds=True"""
        avg = reader.timmean(data_2t, freq='monthly', time_bounds=True)
        assert 'time_bnds' in avg
        assert avg['2t'].shape == (8, 9, 18)
        assert avg['time_bnds'].shape == (avg['2t'].shape[0], 2)
        assert np.all(avg['time_bnds'].isel(bnds=0) <= avg['time_bnds'].isel(bnds=1))

    def test_timmean_invalid_frequency(self, reader, data_2t):
        """Test for timmean method with an invalid frequency"""
        with pytest.raises(ValueError, match=r'Cant find a frequency to resample, using resample_freq=invalid not work, aborting!'):
            reader.timmean(data_2t, freq='invalid')

    def test_timstd_error(self, reader, data_2t):
        """Test for timstd method with a single time step"""
        single = data_2t.sel(time=data_2t.time[0])
        with pytest.raises(ValueError, match=r'Time dimension not found in the input data. Cannot compute timstd statistic'):
            avg = reader.timstat(single, stat='std', freq='monthly')

    def test_timstat_histogram(self, reader, data_2t):
        """Test histogram through timstat"""
        bins, range = 20, (250,330)

        #test passing a string
        hist1 = reader.timstat(data_2t['2t'], freq='monthly', stat='histogram', bins=bins, range=range, exclude_incomplete=True)
        # timhist passes a function
        hist2 = reader.timhist(data_2t['2t'], freq='monthly', bins=bins, range=range, exclude_incomplete=True)
        hist3 = reader.timstat(data_2t['2t'], freq='monthly', stat=histogram, bins=bins, range=range, exclude_incomplete=True)

        assert hist1['center_of_bin'].shape == hist2['center_of_bin'].shape
        assert hist1['center_of_bin'].shape == hist3['center_of_bin'].shape
        assert hist1.isel(time=2).sum().values == hist2.isel(time=2).sum().values
        assert hist2.isel(time=2).sum().values == hist3.isel(time=2).sum().values

        hist1 = reader.timhist(data_2t['2t'], bins=bins, range=range)
        hist2 = reader.histogram(data_2t['2t'], bins=bins, range=range)

        assert hist1.sum().values == hist2.sum().values

    def test_timmean_exclude_incomplete(self, reader, data_2t):
        """Timmean seasonal QS-FEB with exclude_incomplete=True"""
        da = data_2t['2t'].isel(lon=0, lat=0)

        # Use actual data range from 2020
        # Keep full months: Feb, Apr, May, Jun, Jul of 2020; omit March to make Q1 incomplete
        mask = (
            (da.time.dt.year == 2020) &
            (da.time.dt.month.isin([2, 4, 5, 6, 7]))
        )
        da_sel = da.sel(time=mask)
        avg = reader.timmean(da_sel, freq="QS-FEB", exclude_incomplete=True)

        # Only the complete quarter [May, Jun, Jul] should remain
        assert avg.time.dt.strftime("%Y-%m-%d").values[0] == "2020-05-01"

        # Expected value is the mean over May–Jul 2020 of the same series
        expected = da_sel.sel(time=da_sel.time.dt.month.isin([5, 6, 7])).mean().values
        assert float(avg.values) == float(expected)

    def test_timmean_exclude_incomplete_tcoords(self, reader, data_2t):
        """Test that exclude_incomplete mask coordinates align with resampled time axis"""
        da = data_2t['2t'].isel(lon=0, lat=0)

        # Get the averaged result 
        avg_with_mask = reader.timmean(da, freq='daily', exclude_incomplete=True)

        # Get what the resample time axis should be
        expected_time_axis = da.resample(time='1D').mean().time

        # The coordinates should match exactly (subset since incomplete are excluded)
        assert all(t in expected_time_axis.values for t in avg_with_mask.time.values), \
            "Masked result time coordinates should be a subset of the resampled time axis"

        # Verify alignment works without errors - this would fail with mismatched coords
        try:
            resampled = da.resample(time='1D').mean()
            # This operation requires matching coordinates
            aligned = resampled.sel(time=avg_with_mask.time)
            assert len(aligned) == len(avg_with_mask)
        except KeyError as e:
            pytest.fail(f"Coordinate alignment failed: {e}")
"""Test for timmean method"""

import numpy as np
import pytest

from aqua.core.histogram import histogram


def period_key(field, period):
    """Integer label of every time step: YYYYMM ("month") or YYYYMMDD ("day"), the same in every calendar."""
    time = field.time.dt
    if period == "month":
        return time.year.values * 100 + time.month.values
    return time.year.values * 10000 + time.month.values * 100 + time.day.values


def reduce_by_period(series, period, func):
    """Reduce a 1D series over every month or day with plain numpy (independent of the resampling in AQUA)."""
    keys, values = period_key(series, period), series.values  # load the (dask) series once
    return np.array([func(values[keys == key]) for key in np.unique(keys)])


@pytest.fixture(scope="module", params=["long", "long400"])
def source_name(request):
    """Parameterize the source name to run tests for both sources."""
    return request.param


@pytest.fixture(scope="module")
def reader(request, source_name):
    """Picks the correct reader fixture based on source_name."""
    suffix = "long" if source_name == "long" else "long400"
    fixture_name = f"ifs_tco79_{suffix}_fixfalse_reader"
    return request.getfixturevalue(fixture_name)


@pytest.fixture(scope="module")
def data(request, source_name):
    """Picks the correct data fixture based on source_name."""
    suffix = "long" if source_name == "long" else "long400"
    fixture_name = f"ifs_tco79_{suffix}_fixfalse_data"
    return request.getfixturevalue(fixture_name)


@pytest.fixture(scope="module")
def points(data):
    """The series of the cell (lon=0, lat=0) of each variable, loaded once: the reference of the numpy oracles."""
    return {var: data[var].isel(lon=0, lat=0).load() for var in ("2t", "ttr")}


@pytest.fixture(scope="module")
def data_2t(reader):
    """Retrieve only 2t variable (most commonly used)"""
    return reader.retrieve(var="2t")


@pytest.fixture(scope="module")
def data_ttr(reader):
    """Retrieve only ttr variable"""
    return reader.retrieve(var="ttr")


@pytest.mark.aqua
@pytest.mark.xdist_group(name="dask_operations")
class TestTimmean:
    def test_timsum(self, reader, data):
        """Timmean test for sum operation"""
        summed = reader.timsum(data["2t"].isel(lon=0, lat=0), freq="3h")
        assert summed.shape == (1576,)
        assert summed[0] == data["2t"].isel(lon=0, lat=0, time=slice(0, 3)).sum()
        assert np.all(np.unique(summed.time.dt.hour) == np.arange(0, 24, 3))

        with pytest.raises(KeyError, match=r"hypertangent is not a statistic supported by AQUA"):
            reader.timstat(data["2t"], stat="hypertangent", freq="monthly", exclude_incomplete=True)

    @pytest.mark.parametrize("var", ["ttr"])
    def test_timmean_monthly(self, reader, data, points, var):
        """Timmean test for monthly aggregation"""
        avg = reader.timmean(data[var], freq="monthly")
        nmonths = len(np.unique(data.time.dt.month))
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (nmonths, 9, 18)
        assert len(unique) == nmonths
        assert all(counts == counts[0])
        # the mean of each month, from the steps of that month (the first and the last month are partial)
        assert avg.isel(lon=0, lat=0).values == pytest.approx(reduce_by_period(points[var], "month", np.mean))

    @pytest.mark.parametrize("var", ["2t"])
    def test_timstd_allperiod(self, reader, data, points, var):
        """Timstd test for entire data period"""
        avg = reader.timstd(data[var])
        assert avg.shape == (9, 18)
        # population standard deviation (ddof=0), not the sample one
        assert avg.isel(lon=0, lat=0).values == pytest.approx(np.std(points[var].values), rel=1e-6)
        assert avg.isel(lon=0, lat=0).values == pytest.approx(10.011854, rel=1e-6)

    @pytest.mark.parametrize("var", ["2t", "ttr"])
    def test_timstat_monthly_exclude_incomplete(self, reader, data, points, var):
        """Timmean test for monthly aggregation with excluded incomplete chunks"""
        avg = reader.timstat(data[var], stat="mean", freq="monthly", exclude_incomplete=True)
        unique, counts = np.unique(avg.time.dt.month, return_counts=True)
        assert avg.shape == (6, 9, 18)
        assert len(unique) == 6
        assert all(counts == counts[0])
        # February to July are the complete months: the first and the last one are dropped, the others kept as they are
        point = points[var]
        complete = point.sel(time=point.time.dt.month.isin(range(2, 8)))
        assert list(unique) == [2, 3, 4, 5, 6, 7]
        assert avg.isel(lon=0, lat=0).values == pytest.approx(reduce_by_period(complete, "month", np.mean))

    @pytest.mark.parametrize("var", ["2t", "ttr"])
    def test_timmax_daily(self, reader, data, points, var):
        """Timmean test for daily aggregation"""
        avg = reader.timmax(data[var], freq="daily")
        unique, counts = np.unique(avg.time.dt.day, return_counts=True)
        assert avg.shape == (197, 9, 18)
        assert len(unique) == 31
        assert all(
            counts == np.array([7, 7, 7, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 6, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 6, 4])
        )
        assert avg.isel(lon=0, lat=0).values == pytest.approx(reduce_by_period(points[var], "day", np.max))

    def test_timstat_first_last(self, reader, data_2t, points):
        """Selecting first or last value in each group"""
        firstval = data_2t["2t"].aqua.timfirst(freq="daily")
        lastval = data_2t["2t"].aqua.timlast(freq="daily")

        assert firstval.shape == (197, 9, 18)
        assert lastval.shape == (197, 9, 18)
        assert firstval.time[0] < lastval.time[0]
        point = points["2t"]
        assert firstval.isel(lon=0, lat=0).values == pytest.approx(reduce_by_period(point, "day", lambda day: day[0]))
        assert lastval.isel(lon=0, lat=0).values == pytest.approx(reduce_by_period(point, "day", lambda day: day[-1]))

    def test_timmin_yearly_exclude_incomplete(self, reader, data_ttr):
        """Timmean test for yearly aggregation with excluded incomplete chunks"""
        avg = reader.timmin(data_ttr, freq="yearly", exclude_incomplete=True)
        assert avg["ttr"].shape == (0, 9, 18)

    def test_timmean_yearly_center_time(self, reader, data_ttr):
        """Timmean test for yearly aggregation with center_time=True"""
        avg = reader.timmean(data_ttr, freq="yearly", center_time=True)
        assert avg["ttr"].shape == (1, 9, 18)
        # Mid-year of a non-leap year (2020 is leap, 2420 is leap? No, 2400 is, 2420?)
        # 2020: July 2nd 00:00 is the midpoint.
        assert avg["ttr"].time[0].dt.month == 7
        assert avg["ttr"].time[0].dt.day == 2
        assert avg["ttr"].time[0].dt.hour == 0

    def test_timmean_monthly_center_time(self, reader, data_2t):
        """Timmean test for monthly aggregation with center_time=True"""
        avg = reader.timmean(data_2t, freq="monthly", center_time=True)
        assert avg["2t"].shape == (8, 9, 18)
        # Feb 15th 12:00 (for 2020 leap year. 2420 is also leap)
        assert avg["2t"].time[1].dt.month == 2
        assert avg["2t"].time[1].dt.day == 15
        assert avg["2t"].time[1].dt.hour == 12

    def test_timstd_daily_center_time(self, reader, data_2t):
        """Timmean test for daily aggregation with center_time=True and exclude_incomplete=True"""
        avg = reader.timstd(data_2t, freq="daily", center_time=True, exclude_incomplete=True)
        assert avg["2t"].shape == (197, 9, 18)
        # The second day at 12:00
        assert avg["2t"].time[1].dt.hour == 12

    def test_timmean_pandas_accessor(self, reader, data_2t, points):
        """Timmean test for weekly aggregation based on pandas labels"""
        avg = data_2t.aqua.timmean(freq="W-MON")
        assert avg["2t"].shape == (29, 9, 18)
        # pandas weeks end on the Monday of the label, the whole day included: the first one is Monday 20 January only
        point = points["2t"]
        weekly = avg["2t"].isel(lon=0, lat=0)
        days = point.time.values.astype("datetime64[D]")
        for week in (0, 1):
            last_day = weekly.time.values[week].astype("datetime64[D]")
            in_week = (days > last_day - 7) & (days <= last_day)
            assert float(weekly[week]) == pytest.approx(float(point.values[in_week].mean()))
        assert float(weekly[0]) == pytest.approx(250.928362, rel=1e-6)

    def test_timmean_time_bounds(self, reader, data_2t):
        """Test for timmean method with time_bounds=True"""
        avg = reader.timmean(data_2t, freq="monthly", time_bounds=True)
        assert "time_bnds" in avg
        assert avg["2t"].shape == (8, 9, 18)
        assert avg["time_bnds"].shape == (avg["2t"].shape[0], 2)
        assert np.all(avg["time_bnds"].isel(bnds=0) <= avg["time_bnds"].isel(bnds=1))

    @pytest.mark.parametrize("center_time", [False, True])
    @pytest.mark.parametrize("freq, expected_periods", [("monthly", 6), ("yearly", 0)])
    def test_timmean_time_bounds_exclude_incomplete(self, reader, data_2t, center_time, freq, expected_periods):
        """Bounds follow retained periods, including centered and empty outputs."""
        series = data_2t.isel(lon=0, lat=0)
        avg = reader.timmean(series, freq=freq, time_bounds=True, exclude_incomplete=True, center_time=center_time)

        assert avg.sizes["time"] == expected_periods
        assert avg["time_bnds"].shape == (expected_periods, 2)
        assert "history" in avg.attrs
        if expected_periods:
            complete = series.sel(time=series.time.dt.month.isin(range(2, 8)))
            assert avg["time_bnds"].isel(time=0, bnds=0) == complete.time[0]
            assert avg["time_bnds"].isel(time=-1, bnds=1) == complete.time[-1]

    def test_timmean_exclude_incomplete(self, reader, data_2t):
        """Timmean seasonal QS-FEB with exclude_incomplete=True"""
        da = data_2t["2t"].isel(lon=0, lat=0)

        # Use actual data range from the dataset
        target_year = da.time.dt.year[0].values
        # Keep full months: Feb, Apr, May, Jun, Jul; omit March to make Q1 incomplete
        mask = (da.time.dt.year == target_year) & (da.time.dt.month.isin([2, 4, 5, 6, 7]))
        da_sel = da.sel(time=mask)
        avg = reader.timmean(da_sel, freq="QS-FEB", exclude_incomplete=True)

        # Only the complete quarter [May, Jun, Jul] should remain
        assert avg.time.dt.month[0] == 5
        assert avg.time.dt.day[0] == 1

        # Expected value is the mean over May–Jul 2020 of the same series
        expected = da_sel.sel(time=da_sel.time.dt.month.isin([5, 6, 7])).mean().values
        assert float(avg.isel(time=0).values) == float(expected)

    def test_timmean_exclude_incomplete_tcoords(self, reader, data_2t):
        """Test that exclude_incomplete mask coordinates align with resampled time axis"""
        da = data_2t["2t"].isel(lon=0, lat=0)

        # Get the averaged result
        avg_with_mask = reader.timmean(da, freq="daily", exclude_incomplete=True)

        # Get what the resample time axis should be
        expected_time_axis = da.resample(time="1D").mean().time

        # The coordinates should match exactly (subset since incomplete are excluded)
        assert all(t in expected_time_axis.values for t in avg_with_mask.time.values), (
            "Masked result time coordinates should be a subset of the resampled time axis"
        )

        # Verify alignment works without errors - this would fail with mismatched coords
        try:
            resampled = da.resample(time="1D").mean()
            # This operation requires matching coordinates
            aligned = resampled.sel(time=avg_with_mask.time)
            assert len(aligned) == len(avg_with_mask)
        except KeyError as e:
            pytest.fail(f"Coordinate alignment failed: {e}")


@pytest.mark.aqua
@pytest.mark.xdist_group(name="dask_operations")
class TestTimstatAnyCalendar:
    """Checks that do not depend on the calendar of the data: they run on the standard source only."""

    @pytest.fixture(scope="class")
    def reader(self, ifs_tco79_long_fixfalse_reader):
        return ifs_tco79_long_fixfalse_reader

    @pytest.fixture(scope="class")
    def data_2t(self, reader):
        return reader.retrieve(var="2t")

    def test_timstat_attrs(self, reader, data_2t):
        """Time statistics keep the units and the AQUA attributes and log what was computed"""
        monthly = reader.timmean(data_2t["2t"], freq="monthly")
        assert monthly.attrs["units"] == "K"
        assert monthly.attrs["AQUA_model"] == "IFS"
        assert "resampled from frequency <Hour> to frequency monthly by AQUA timmean" in monthly.attrs["history"]

        daily = reader.timmax(data_2t["2t"], freq="daily")
        assert daily.attrs["units"] == "K"
        assert "resampled from frequency <Hour> to frequency daily by AQUA timmax" in daily.attrs["history"]

    @pytest.mark.parametrize("stat", ["first", "last"])
    def test_timstat_first_last_no_freq(self, reader, data_2t, stat):
        """first and last statistics require a frequency to be specified"""
        with pytest.raises(ValueError, match=r"Frequency must be specified when using first or last statistic"):
            reader.timstat(data_2t["2t"], stat=stat)

    def test_timstat_compare(self, reader, data_2t):
        """Time operations provide robust values"""
        minval = reader.timmin(data_2t["2t"], freq="daily")
        maxval = reader.timmax(data_2t["2t"], freq="daily")
        avg = reader.timmean(data_2t["2t"], freq="daily")

        assert (minval <= avg).all()
        assert (avg <= maxval).all()

    def test_timmean_time_bounds_no_freq(self, reader, data_2t):
        """Total means disable time bounds and other resampling-only options."""
        avg = reader.timmean(data_2t, freq=None, time_bounds=True, center_time=True, exclude_incomplete=True)
        assert "time" not in avg.dims
        assert "time_bnds" not in avg

    def test_timmean_invalid_frequency(self, reader, data_2t):
        """Test for timmean method with an invalid frequency"""
        with pytest.raises(
            ValueError, match=r"Cannot find a frequency to resample, using resample_freq=invalid not work, aborting!"
        ):
            reader.timmean(data_2t, freq="invalid")

    def test_timstd_error(self, reader, data_2t):
        """Test for timstd method with a single time step"""
        single = data_2t.sel(time=data_2t.time[0])
        with pytest.raises(ValueError, match=r"Time dimension not found in the input data. Cannot compute timstd statistic"):
            reader.timstat(single, stat="std", freq="monthly")

    def test_timstat_histogram(self, reader, data_2t):
        """Test histogram through timstat"""
        bins, range = 20, (250, 330)

        # test passing a string
        hist1 = reader.timstat(
            data_2t["2t"], freq="monthly", stat="histogram", bins=bins, range=range, exclude_incomplete=True
        )
        # timhist passes a function
        hist2 = reader.timhist(data_2t["2t"], freq="monthly", bins=bins, range=range, exclude_incomplete=True)
        hist3 = reader.timstat(data_2t["2t"], freq="monthly", stat=histogram, bins=bins, range=range, exclude_incomplete=True)

        assert hist1["center_of_bin"].shape == hist2["center_of_bin"].shape
        assert hist1["center_of_bin"].shape == hist3["center_of_bin"].shape
        assert hist1.isel(time=2).sum().values == hist2.isel(time=2).sum().values
        assert hist2.isel(time=2).sum().values == hist3.isel(time=2).sum().values

        hist1 = reader.timhist(data_2t["2t"], bins=bins, range=range)
        hist2 = reader.histogram(data_2t["2t"], bins=bins, range=range)

        assert hist1.sum().values == hist2.sum().values

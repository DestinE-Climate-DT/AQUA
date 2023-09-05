"""Tests for streaming"""

import pytest
import pandas as pd
from aqua import Reader

# pytest approximation, to bear with different machines
approx_rel = 1e-4


@pytest.fixture(scope="function")
def reader_instance():
    return Reader(model="IFS", exp="test-tco79", source="long",
                  fix=False)


# streaming class for tests
@pytest.mark.aqua
class TestAquaStreaming:
    """The streamin testing class"""

    @pytest.fixture(params=["hours", "days", "months"])
    def stream_units(self, request):
        return request.param

    @pytest.fixture(params=["2020-01-20", "2020-03-05"])
    def stream_date(self, request):
        return request.param

    @pytest.fixture(scope="function",
                    params=[{"stream_step": 3},
                            {"stream_step": 3, "stream_unit": "days"},
                            {"stream_step": 3, "stream_unit": "days", "stream_startdate": "2020-01-20"},
                            {"stream_step": 3}])
    def stream_args(self, request, stream_date, stream_units):
        req = request.param
        req.update({"streaming": True})
        req["stream_startdate"] = stream_date
        req["stream_unit"] = stream_units
        return req

    def test_stream_retrieve(self, reader_instance, stream_units, stream_date,
                             stream_args):
        """
        Test if the retrieve method returns streamed data with streaming=true
        changing start date
        """

        reader = reader_instance
        start_date = pd.to_datetime(stream_date)
        if stream_units == "steps":
            offset = pd.DateOffset(**{"hours": 3})
        else:
            offset = pd.DateOffset(**{stream_units: 3})
        step = pd.DateOffset(hours=1)

        dates = pd.date_range(start=start_date, end=start_date+offset,
                              freq='1H')
        num_hours = (dates[-1] - dates[0]).total_seconds() / 3600

        data = reader.retrieve(**stream_args)
        # Test if it has the right size
        assert data['2t'].shape == (num_hours, 9, 18)
        # Test if starting date is ok
        assert data.time.values[0] == start_date
        # Test if end date is ok
        assert data.time.values[-1] == start_date + offset - step

        # Test if we can go to the next date
        data = reader.retrieve(**stream_args)
        assert data.time.values[0] == start_date + offset

        # Test if reset_stream works
        reader.reset_stream()
        data = reader.retrieve(**stream_args)
        assert data.time.values[0] == start_date

        # test generator
        reader.reset_stream()
        stream_args_copy = stream_args.copy()
        stream_args_copy.update({"streaming_generator": True})
        data_gen = reader.retrieve(**stream_args_copy)
        # Test if at beginning
        assert next(data_gen).time.values[0] == start_date
        # Test if at next date
        assert next(data_gen).time.values[0] == start_date + offset

    @pytest.fixture(scope="function",
                    params=[{"stream_step": 3, "stream_unit": "days"},
                            {"stream_step": 3, "stream_unit": "days", "stream_startdate": "2020-01-20"},
                            {"stream_step": 3}])
    def reader_instance_with_args(self, request, stream_date, stream_units):
        req = request.param
        req.update({"streaming": True, "model": "IFS", "exp": "test-tco79",
                    "source": "long", "fix": False})
        req["stream_unit"] = stream_units
        req["stream_startdate"] = stream_date
        return Reader(**req)

    def test_stream_reader(self, reader_instance_with_args, stream_units,
                           stream_date):
        """
        Test if the retrieve method returns streamed data with streaming=true
        changing start date
        """

        reader = reader_instance_with_args
        start_date = pd.to_datetime(stream_date)
        if stream_units == "steps":
            offset = pd.DateOffset(**{"hours": 3})
        else:
            offset = pd.DateOffset(**{stream_units: 3})
        step = pd.DateOffset(hours=1)

        dates = pd.date_range(start=start_date, end=start_date+offset,
                              freq='1H')
        num_hours = (dates[-1] - dates[0]).total_seconds() / 3600

        data = reader.retrieve()
        # Test if it has the right size
        assert data['2t'].shape == (num_hours, 9, 18)
        # Test if starting date is ok
        assert data.time.values[0] == start_date
        # Test if end date is ok
        assert data.time.values[-1] == start_date + offset - step

        # Test if we can go to the next date
        data = reader.retrieve()
        assert data.time.values[0] == start_date + offset

        # Test if reset_stream works
        reader.reset_stream()
        data = reader.retrieve()
        assert data.time.values[0] == start_date

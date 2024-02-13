"""Tests for streaming"""

import pytest
import pandas as pd
from aqua import Reader

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = "DEBUG"

# streaming class for tests
@pytest.mark.aqua
class TestAquaStreaming:
    """The streaming testing class"""

    @pytest.fixture(scope="function",
                    params=[{"aggregation": '3S', "startdate": "2020-01-20"},
                            {"aggregation": 'daily', "startdate": "2020-05-01"},
                            {"aggregation": '3D', "startdate": "2020-05-01", "enddate": "2020-05-02"}
                            ])
    def reader_instance_with_args(self, request):
        req = request.param
        req.update({"streaming": True, "model": "IFS", "exp": "test-tco79",
                    "source": "long", "fix": False, "regrid":False})
        return Reader(**req)

    def test_stream(self, reader_instance_with_args):
        """
        Test if the retrieve method returns streamed data with streaming=true
        changing start date and end date
        """

        reader = reader_instance_with_args

        if 'S' in reader.aggregation:
            offset = pd.DateOffset(**{"hours": 3})
        if 'D' in reader.aggregation:
            offset = pd.DateOffset(**{"days": 3})
        if 'daily' in reader.aggregation:
            offset = pd.DateOffset(**{"days": 1})

        step = pd.DateOffset(hours=1)
        
        start_date = pd.to_datetime(reader.startdate)
        if reader.enddate:
            end_date = pd.to_datetime(reader.enddate) + pd.Timedelta(days=1)
        else:
            end_date = start_date + offset

        dates = pd.date_range(start=start_date, end=end_date, freq='h')          
            
        num_hours = (dates[-1] - dates[0]).total_seconds() / 3600
        
        data = reader.retrieve()

         # Test if it has the right size
        assert data['2t'].shape == (num_hours, 9, 18)

        # Test if starting date is ok
        assert data.time.values[0] == pd.to_datetime(start_date)
        
        # Test if end date is ok
        assert data.time.values[-1] == pd.to_datetime(end_date - step)

        # Test if we can go to the next date
        data = reader.retrieve()
        if not reader.enddate:
            assert data.time.values[0] == start_date + offset

        # Test if reset_stream works
        reader.reset_stream()
        data = reader.retrieve()
        assert data.time.values[0] == start_date


    def test_generator(self):
        """
        Test generator option of streaming mode
        """
        reader = Reader(model='IFS', exp='test-tco79', source='long', fix = False, stream_generator = True, startdate = '2020-05-01', aggregation = 'monthly')
        data_gen = reader.retrieve()
        start_dates = ["2020-05-01", "2020-06-01", "2020-07-01", "2020-08-01"]
        i = 0
        for data in data_gen:
            assert data.time[0].values == pd.to_datetime(start_dates[i])
            i+=1

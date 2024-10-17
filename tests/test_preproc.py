import pytest
import xarray
import numpy as np
import pandas as pd

from aqua import Reader

loglevel = "DEBUG"

@pytest.fixture(
    params=[
        ("IFS", "test-tco79", "short_masked", "2t")
    ]
)

def reader_arguments(request):
    return request.param

def double(x : xarray.Dataset) -> xarray.Dataset:
    return x * 2

def shift_time(x :xarray.Dataset) -> xarray.Dataset:
    """ Shift the time of 10 years """
    shifted_time = pd.to_datetime(x['time'].values) + pd.DateOffset(years=1)

    shifted_data = x.copy(deep=True)
    shifted_data['time'] = shifted_time

    return shifted_data

@pytest.mark.aqua
class TestPreproc():
    """Test different preprocessing functions"""

    def test_preproc_double(self, reader_arguments):
        """
        Test a preprocess doubling the data
        """
        model, exp, source, variable = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel)
        data = reader.retrieve()
        
        reader_preproc = Reader(model=model, exp=exp, source=source,
                                preproc=double, loglevel=loglevel)
        data_preproc = reader_preproc.retrieve()

        assert (data[variable] * 2).equals(data_preproc[variable])

    def test_preproc_shift_time(self, reader_arguments):
        """
        Test a preprocess shifting the time of 1 year
        """
        model, exp, source, _ = reader_arguments

        reader = Reader(model=model, exp=exp, source=source, loglevel=loglevel)
        data = reader.retrieve()
        
        reader_preproc = Reader(model=model, exp=exp, source=source,
                                preproc=shift_time, loglevel=loglevel)
        data_preproc = reader_preproc.retrieve()

        assert np.array_equal((pd.to_datetime(data['time'].values) + pd.DateOffset(years=1)).values,
                             data_preproc['time'].values)
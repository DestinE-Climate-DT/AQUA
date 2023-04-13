"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from aqua import Reader, catalogue


@pytest.fixture(params=[(model, exp, source)
                        for model in catalogue()
                        for exp in catalogue()[model]
                        for source in catalogue()[model][exp]])
def reader(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    print([model, exp, source])
    # very slow access, skipped
    if model == 'ICON' and source == 'intake-esm-test':
        pytest.skip()
    if model == 'MSWEP' and source == 'daily':
        pytest.skip()
    if model == 'MSWEP' and source == '3hourly':
        pytest.skip()
    myread = Reader(model=model, exp=exp, source=source, areas=False)
    data = myread.retrieve(fix=False)
    return myread, data


def test_catalog(reader):
    """Checking that both reader and Dataset are retrived in reasonable shape"""
    a, b = reader
    assert isinstance(a, Reader)
    assert isinstance(b, xarray.Dataset)

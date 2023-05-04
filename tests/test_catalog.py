"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from aqua import Reader, catalogue, inspect_catalogue


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

def test_catalogue(reader):
    """Checking that both reader and Dataset are retrived in reasonable shape"""
    aaa, bbb = reader
    assert isinstance(aaa, Reader)
    assert isinstance(bbb, xarray.Dataset)

def test_inspect_catalogue():
    """Checking that inspect catalogue works"""
    cat = catalogue(verbose=True)
    models = inspect_catalogue(cat)
    assert isinstance(models, list)
    exps = inspect_catalogue(cat, model='IFS')
    assert isinstance(exps, list)
    sources = inspect_catalogue(cat, model='IFS', exp='test-tco79')
    assert isinstance(sources, list)

"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from aqua import Reader, catalogue, inspect_catalogue
from aqua.reader.reader_utils import check_catalog_source

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

@pytest.mark.slow
def test_catalogue(reader):
    """Checking that both reader and Dataset are retrived in reasonable shape"""
    aaa, bbb = reader
    assert isinstance(aaa, Reader)
    assert isinstance(bbb, xarray.Dataset)

@pytest.mark.aqua
def test_inspect_catalogue():
    """Checking that inspect catalogue works"""
    cat = catalogue(verbose=True)
    models = inspect_catalogue(cat)
    assert isinstance(models, list)
    exps = inspect_catalogue(cat, model='IFS')
    assert isinstance(exps, list)
    sources = inspect_catalogue(cat, model='IFS', exp='test-tco79')
    assert isinstance(sources, list)

@pytest.mark.aqua
@pytest.mark.parametrize(
    "cat, model, exp, source, expected_output",
    [
        # Test case 1: Source is specified and exists in the catalog
        (
            {"model1": {"exp1": {"source1": "data1", "source2": "data2"}}},
            "model1",
            "exp1",
            "source1",
            "source1"
        ),
        # Test case 2: Source is specified but does not exist, default source exists
        (
            {"model1": {"exp1": {"default": "default_data", "source2": "data2"}}},
            "model1",
            "exp1",
            "source1",
            "default"
        ),
        # Test case 3: Source is specified but does not exist, default source does not exist
        (
            {"model1": {"exp1": {"source2": "data2"}}},
            "model1",
            "exp1",
            "source1",
            pytest.raises(KeyError)
        ),
        # Test case 4: Source is not specified, choose the first source
        (
            {"model1": {"exp1": {"source1": "data1", "source2": "data2"}}},
            "model1",
            "exp1",
            None,
            "source1"
        ),
        # Test case 5: Source is not specified, no sources available
        (
            {"model1": {"exp1": {}}},
            "model1",
            "exp1",
            None,
            pytest.raises(KeyError)
        ),
        # Test case 6: Source is not specified, no sources available, but a default source exists
        (
            {"model1": {"exp1": {"default": "default_data"}}},
            "model1",
            "exp1",
            None,
            "default"
        )
    ]
)
def test_check_catalog_source(cat, model, exp, source, expected_output):
    if isinstance(expected_output, str):
        assert check_catalog_source(cat, model, exp, source) == expected_output
    else:
        with expected_output:
            check_catalog_source(cat, model, exp, source)
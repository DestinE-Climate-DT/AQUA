"""Test checking if all catalog entries can be read"""

import pytest
import types
import xarray
from aqua import Reader, catalog, inspect_catalog
from aqua.reader.reader_utils import check_catalog_source

loglevel = "DEBUG"

@pytest.fixture(params=[(model, exp, source)
                        for model in catalog()
                        for exp in catalog()[model]
                        for source in catalog()[model][exp]])
def reader(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    print([model, exp, source])
    # very slow access, skipped
    if model == 'ICON' and source == 'intake-esm-test':
        pytest.skip()
    if model == 'ICON' and exp == 'hpx':
        pytest.skip()
    if model == 'MSWEP':
        pytest.skip()
    if model == 'ERA5':
        pytest.skip()
    if model == 'IFS' and exp == 'test-fdb':
        pytest.skip()
    # teleconnections catalog, only on teleconnections workflow
    if model == 'IFS' and exp == 'test-tco79' and source == 'teleconnections':
        pytest.skip()
    myread = Reader(model=model, exp=exp, source=source, areas=False,
                    fix=False, loglevel=loglevel)
    data = myread.retrieve()
    return myread, data

@pytest.mark.gsv
def test_catalog_gsv(reader):
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    sources = ['fdb', 'fdb-levels', 'fdb-nolevels']

    for source in sources:
        reader_gsv = Reader(model='IFS', exp='test-fdb', source=source,
                            loglevel=loglevel)
        data = reader_gsv.retrieve()

        assert isinstance(reader_gsv, Reader)
        assert isinstance(data, xarray.Dataset)

@pytest.fixture(params=[(model, exp, source)
                        for model in catalog()
                        for exp in catalog()[model]
                        for source in catalog()[model][exp]])
def reader_regrid(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    print([model, exp, source])
    # very slow access, skipped
    if model == 'ICON' and source == 'intake-esm-test':
        pytest.skip()
    if model == 'ICON' and exp == 'hpx':
        pytest.skip()
    if model == 'MSWEP':
        pytest.skip()
    if model == 'ERA5':
        pytest.skip()
    if model == 'IFS' and source == 'fdb':  # there is another test for that
        pytest.skip()
    # teleconnections catalog, only on teleconnections workflow
    if model == 'IFS' and exp == 'test-tco79' and source == 'teleconnections':
        pytest.skip()
    myread = Reader(model=model, exp=exp, source=source, areas=True, regrid='r200',
                    loglevel=loglevel, rebuild=False)
    data = myread.retrieve()

    return myread, data


@pytest.mark.slow
def test_catalog(reader):
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    aaa, bbb = reader
    assert isinstance(aaa, Reader)
    try:
        assert isinstance(bbb, xarray.Dataset)
    except AssertionError: #fdb is a generator
        assert isinstance(bbb, types.GeneratorType)

@pytest.mark.sbatch
def test_catalog_reader(reader_regrid):
    """
    Checking that data can be regridded
    """
    read, data = reader_regrid
    vvv = list(data.data_vars)[-1]
    select = data[vvv].isel(time=0)
    rgd = read.regrid(select)
    assert len(rgd.lon) == 180
    assert len(rgd.lat) == 90


@pytest.mark.aqua
def test_inspect_catalog():
    """Checking that inspect catalog works"""

    # calling the catalog
    #cat = catalog(verbose=True)
    #out, _ = capfd.readouterr()
    #assert 'FESOM' in out
    #assert 'IFS' in out

    # inspect catalog
    models = inspect_catalog()
    assert isinstance(models, list)
    exps = inspect_catalog(model='IFS')
    assert isinstance(exps, list)
    sources = inspect_catalog(model='IFS', exp='test-tco79')
    assert isinstance(sources, list)
    variables = inspect_catalog(model='IFS', exp="test-tco79", source='short')
    assert variables is True

    # wrong calls
    models = inspect_catalog(model='antani')
    assert 'IFS' in models 
    exps = inspect_catalog(model='IFS', exp="antani")
    assert 'test-tco79' in exps
    sources = inspect_catalog(model='IFS', exp="test-tco79", source='antani')
    assert 'short' in sources

# @pytest.mark.aqua
# @pytest.mark.parametrize(
#     "catalog, model, exp, source, expected_output",
#     [
#         # Test case 1: Source is specified and exists in the catalog
#         (
#             {"model1": {"exp1": {"source1": "data1", "source2": "data2"}}},
#             "model1",
#             "exp1",
#             "source1",
#             "source1"
#         ),
#         # Test case 2: Source is specified but does not exist,
#         # default source exists
#         (
#             {"model1": {"exp1": {"default": "default_data", "source2": "data2"}}},
#             "model1",
#             "exp1",
#             "source1",
#             "default"
#         ),
#         # Test case 3: Source is specified but does not exist,
#         # default source does not exist
#         (
#             {"model1": {"exp1": {"source2": "data2"}}},
#             "model1",
#             "exp1",
#             "source1",
#             pytest.raises(KeyError)
#         ),
#         # Test case 4: Source is not specified, choose the first source
#         (
#             {"model1": {"exp1": {"source1": "data1", "source2": "data2"}}},
#             "model1",
#             "exp1",
#             None,
#             "source1"
#         ),
#         # Test case 5: Source is not specified, no sources available
#         (
#             {"model1": {"exp1": {}}},
#             "model1",
#             "exp1",
#             None,
#             pytest.raises(KeyError)
#         ),
#         # Test case 6: Source is not specified, no sources available,
#         # but a default source exists
#         (
#             {"model1": {"exp1": {"default": "default_data"}}},
#             "model1",
#             "exp1",
#             None,
#             "default"
#         )
#     ]
# )
# def test_check_catalog_source(catalog, model, exp, source, expected_output):
#     if isinstance(expected_output, str):
#         assert check_catalog_source(catalog, model, exp, source) == expected_output
#     else:
#         with expected_output:
#             check_catalog_source(catalog, model, exp, source)

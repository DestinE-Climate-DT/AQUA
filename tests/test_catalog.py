"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from aqua import Reader
from aqua.core.reader import show_catalog_content as catalog
from conftest import LOGLEVEL


@pytest.fixture(params=[(model, exp, source)
                        for model in catalog(catalog_name="ci", verbose=False)['ci']
                        for exp in catalog(catalog_name="ci", verbose=False)['ci'][model]
                        for source in catalog(catalog_name="ci", verbose=False)['ci'][model][exp]])
def reader(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    if source == 'intake-esm-test': # temporary skip of intake esm sources
        pytest.skip("Skipping intake-esm-test for now, not supported for now")
    myread = Reader(catalog='ci', model=model, exp=exp, source=source, areas=False,
                    fix=False, loglevel=LOGLEVEL)
    data = myread.retrieve()
    return myread, data

@pytest.mark.gsv
def test_catalog_gsv():
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    sources = ['fdb', 'fdb-levels', 'fdb-nolevels']

    for source in sources:
        reader_gsv = Reader(model='IFS', exp='test-fdb', source=source,
                            loglevel=LOGLEVEL)
        data = reader_gsv.retrieve()

        assert isinstance(reader_gsv, Reader)
        assert isinstance(data, xarray.Dataset)

@pytest.fixture(params=[(model, exp, source)
                        for model in catalog(catalog_name="ci", verbose=False)['ci']
                        for exp in catalog(catalog_name="ci")['ci'][model]
                        for source in catalog(catalog_name="ci")['ci'][model][exp]])
def reader_regrid(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    print([model, exp, source])
    myread = Reader(catalog='ci', model=model, exp=exp, source=source, areas=True, regrid='r200',
                    loglevel=LOGLEVEL, rebuild=False)
    data = myread.retrieve()

    return myread, data


@pytest.mark.slow
def test_catalog(reader):
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    aaa, bbb = reader
    assert isinstance(aaa, Reader)
    assert isinstance(bbb, xarray.Dataset)


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

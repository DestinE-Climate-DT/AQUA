"""Test checking if all catalog entries can be read"""

import pytest
import types
import xarray
from aqua import Reader, aqua_catalog, inspect_catalog

loglevel = "DEBUG"

@pytest.fixture(params=[(catalog, model, exp, source)
                        for catalog in aqua_catalog(verbose=False)
                        for model in aqua_catalog(verbose=False)[catalog]
                        for exp in aqua_catalog(verbose=False)[catalog][model]
                        for source in aqua_catalog(verbose=False)[catalog][model][exp]])
def reader(request):
    """Reader instance fixture"""
    catalog, model, exp, source = request.param
    print([catalog, model, exp, source])
    # very slow access, skipped
    # if model == 'ICON' and source == 'intake-esm-test':
    #     pytest.skip()
    # if model == 'ICON' and exp == 'hpx':
    #     pytest.skip()
    # if model == 'MSWEP':
    #     pytest.skip()
    # if model == 'ERA5':
    #     pytest.skip()
    # if model == 'IFS' and exp == 'test-fdb':
    #     pytest.skip()
    # # teleconnections catalog, only on teleconnections workflow
    # if model == 'IFS' and exp == 'test-tco79' and source == 'teleconnections':
    #     pytest.skip()
    myread = Reader(catalog=catalog, model=model, exp=exp, source=source, areas=False,
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

@pytest.fixture(params=[(catalog, model, exp, source)
                        for catalog in aqua_catalog(verbose=False)
                        for model in aqua_catalog(verbose=False)[catalog]
                        for exp in aqua_catalog(verbose=False)[catalog][model]
                        for source in aqua_catalog(verbose=False)[catalog][model][exp]])
def reader_regrid(request):
    """Reader instance fixture"""
    catalog, model, exp, source = request.param
    print([catalog, model, exp, source])
    # very slow access, skipped
    # if model == 'ICON' and source == 'intake-esm-test':
    #     pytest.skip()
    # if model == 'ICON' and exp == 'hpx':
    #     pytest.skip()
    # if model == 'MSWEP':
    #     pytest.skip()
    # if model == 'ERA5':
    #     pytest.skip()
    # if model == 'IFS' and source == 'fdb':  # there is another test for that
    #     pytest.skip()
    # # teleconnections catalog, only on teleconnections workflow
    # if model == 'IFS' and exp == 'test-tco79' and source == 'teleconnections':
    #     pytest.skip()
    myread = Reader(catalog=catalog, model=model, exp=exp, source=source, areas=True, regrid='r200',
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

    # inspect catalog
    catalogs = inspect_catalog()
    assert isinstance(catalogs, list)
    assert 'ci' in catalogs

    models = inspect_catalog(catalog='ci')
    assert isinstance(models, list)

    exps = inspect_catalog(model='IFS')
    assert isinstance(exps, list)

    sources = inspect_catalog(model='IFS', exp='test-tco79')
    assert isinstance(sources, list)

    variables = inspect_catalog(model='IFS', exp="test-tco79", source='short')
    assert variables is True

    #wrong calls
    models = inspect_catalog(catalog='ci', model='antani')
    assert 'IFS' in models

    exps = inspect_catalog(model='IFS', exp="antani")
    assert 'test-tco79' in exps

    sources = inspect_catalog(model='IFS', exp="test-tco79", source='antani')
    assert 'short' in sources

    #errors
    with pytest.raises(ValueError):
        inspect_catalog(exp='antani')
    with pytest.raises(ValueError):
        inspect_catalog(model='pippo', source='pluto')

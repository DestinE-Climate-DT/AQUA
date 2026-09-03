"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from conftest import LOGLEVEL

from aqua import Reader

# from aqua.core.intake_drivers.fdb.openers.gsv_source import gsv_available
from aqua.core.reader import show_catalog_content as catalog


def _catalog_params(marks_for_source):
    cat = catalog(catalog_name="ci", verbose=False)["ci"]
    return [
        pytest.param(
            (model, exp, source),
            marks=marks_for_source(source),
            id=f"{model}-{exp}-{source}",
        )
        for model in cat
        for exp in cat[model]
        for source in cat[model][exp]
    ]


def _default_marks(source):
    return [pytest.mark.fdb] if "fdb" in source else [pytest.mark.aqua]


# -- FIXTURES --- #


@pytest.fixture(params=_catalog_params(_default_marks))
def reader(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    if source == "intake-esm-test":  # temporary skip of intake esm sources
        pytest.skip("Skipping intake-esm-test for now, not supported for now")
    # if not gsv_available and "fdb" in source:
    #     pytest.skip(f"Skipping {model} {exp} {source} because GSV is not available")
    myread = Reader(catalog="ci", model=model, exp=exp, source=source, areas=False, fix=False, loglevel=LOGLEVEL)
    data = myread.retrieve()
    return myread, data


@pytest.fixture(params=_catalog_params(_default_marks))
def reader_regrid(request):
    """Reader instance fixture"""
    model, exp, source = request.param
    if source == "intake-esm-test":  # temporary skip of intake esm sources
        pytest.skip("Skipping intake-esm-test for now, not supported for now")
    # if not gsv_available and "fdb" in source:
    #     pytest.skip(f"Skipping {model} {exp} {source} because GSV is not available")
    myread = Reader(
        catalog="ci", model=model, exp=exp, source=source, areas=True, regrid="r200", loglevel=LOGLEVEL, rebuild=False
    )
    data = myread.retrieve()

    return myread, data


# --- TESTS --- #


@pytest.mark.fdb
def test_catalog_gsv():
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    sources = ["fdb", "fdb-levels", "fdb-nolevels"]

    for source in sources:
        reader_gsv = Reader(model="IFS", exp="test-fdb", source=source, loglevel=LOGLEVEL)
        data = reader_gsv.retrieve()

        assert isinstance(reader_gsv, Reader)
        assert isinstance(data, xarray.Dataset)


# reader test, get markers from fixture
def test_catalog(reader):
    """
    Checking that both reader and Dataset are retrived in reasonable shape
    """
    aaa, bbb = reader
    assert isinstance(aaa, Reader)
    assert isinstance(bbb, xarray.Dataset)


# reader test, get markers from fixture
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

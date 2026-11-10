"""Test checking if all catalog entries can be read"""

import pytest
import xarray
from utils_tests import LOGLEVEL

from aqua import Reader
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
    myread = Reader(catalog="ci", model=model, exp=exp, source=source, areas=False, fix=False, loglevel=LOGLEVEL)
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
    _, data = reader
    assert isinstance(data, xarray.Dataset)
    assert len(data.data_vars) > 0
    assert all(size > 0 for size in data.sizes.values())

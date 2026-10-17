"""Tests for streaming"""

import pytest
from utils_tests import LOGLEVEL

from aqua import Reader

loglevel = LOGLEVEL


@pytest.fixture(scope="module")
def reader():
    return Reader(model="FESOM", exp="test-pi", source="original_3d", loglevel=loglevel)


@pytest.fixture(scope="module")
def data(reader):
    """Retrieve 3D FESOM data once for all vertinterp tests"""
    return reader.retrieve()


@pytest.mark.aqua
def test_vertinterp(reader, data):
    """Trivial test for vertical interpolation. to be expanded"""
    select = data.isel(time=0)

    # dataarray access
    interp = reader.vertinterp(select["thetao"], levels=10, units="m", level_coord="nz1")

    assert pytest.approx(interp[40].values) == 272.64060

    # dataset access
    interp = reader.vertinterp(select, levels=10, level_coord="nz1")
    assert pytest.approx(interp["thetao"][40].values) == 272.64060

    # change unit
    interp = reader.vertinterp(select["thetao"], levels=[0.1, 0.3], units="km", level_coord="nz1")
    assert interp.shape == (2, 3140)


@pytest.mark.aqua
def test_vertinterp_exceptions(reader, data):
    """ "Test exceptions for vertical interpolation"""
    select = data.isel(time=0)

    # wrong level_coord
    with pytest.raises(KeyError):
        reader.vertinterp(select["ocpt"], levels=10, units="m", level_coord="nz2")

    # no levels
    with pytest.raises(KeyError):
        reader.vertinterp(select["ocpt"], units="m", level_coord="nz1")


@pytest.mark.aqua
@pytest.mark.parametrize("as_dataarray", [False, True], ids=["dataset", "dataarray"])
def test_vertinterp_preserves_reader_without_changing_default(reader, data, ifs_tco79_short_r200_reader, as_dataarray):
    """Vertical interpolation binds its output without resetting an unrelated default."""
    field = data[["thetao"]].isel(time=0)
    if as_dataarray:
        field = field["thetao"]
    ifs_tco79_short_r200_reader.set_default()
    result = reader.vertinterp(field, levels=10, units="m", level_coord="nz1")
    assert result.aqua.instance is reader
    assert Reader.instance is ifs_tco79_short_r200_reader
    values = result if as_dataarray else result["thetao"]
    assert values[40].compute().item() == pytest.approx(272.64060)

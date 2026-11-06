"""Tests for streaming"""

import numpy as np
import pytest
from utils_tests import LOGLEVEL

from aqua import Reader


@pytest.fixture(scope="module")
def reader():
    return Reader(model="FESOM", exp="test-pi", source="original_3d", loglevel=LOGLEVEL)


@pytest.fixture(scope="module")
def data(reader):
    """Retrieve 3D FESOM data once for all vertinterp tests"""
    return reader.retrieve()


def interp_columns(field, depth, levels):
    """Linear interpolation of every column (nz1, x) of a field at the given depths, with numpy."""
    return np.array([np.interp(levels, depth, column) for column in field.values.T]).T


@pytest.mark.aqua
def test_vertinterp(reader, data):
    """Vertical interpolation of the temperature, in the units of the data and in other units"""
    select = data.isel(time=0)
    depth = select["nz1"].values  # 2.5, 125 and 2275 m

    # dataarray access
    interp = reader.vertinterp(select["thetao"], levels=10, units="m", level_coord="nz1")

    assert pytest.approx(interp[40].values) == 272.64060
    assert interp.values == pytest.approx(interp_columns(select["thetao"], depth, 10))  # independent oracle

    # dataset access
    interp = reader.vertinterp(select, levels=10, level_coord="nz1")
    assert pytest.approx(interp["thetao"][40].values) == 272.64060

    # change unit: 0.1 and 0.3 km are 100 and 300 m
    interp = reader.vertinterp(select["thetao"], levels=[0.1, 0.3], units="km", level_coord="nz1")
    assert interp.shape == (2, 3140)
    assert interp.values == pytest.approx(interp_columns(select["thetao"], depth, [100, 300]))
    assert interp.values[:, 40] == pytest.approx([272.14449, 272.09974])


@pytest.mark.aqua
def test_vertinterp_exceptions(reader, data):
    """ "Test exceptions for vertical interpolation"""
    select = data.isel(time=0)

    # wrong level_coord
    with pytest.raises(KeyError, match="The level_coord=nz2 is not in the data!"):
        reader.vertinterp(select["thetao"], levels=10, units="m", level_coord="nz2")

    # no levels
    with pytest.raises(KeyError, match="Levels for interpolation must be specified"):
        reader.vertinterp(select["thetao"], units="m", level_coord="nz1")

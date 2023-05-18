"""Test fixer functionality for Reader"""

import pytest
from aqua import Reader

@pytest.mark.aqua
#@pytest.mark.skip(reason="excluded from GH actions")
def test_fixer():
    """Test basic fixing"""

    ntime = [10, 20, 1000]  # points in time to be checked (includes 1 month jump)
    reader = Reader(model="IFS", exp="test-tco79", source="long")
    data0 = reader.retrieve(var=['2t', 'ttr'], fix=False)  # Retrieve not fixed data
    ttr0 = data0.ttr[ntime, 0, 0]
    tas0 = data0['2t'][ntime, 5, 5]

    # Preliminary - did we read the correct values?
    assert pytest.approx(tas0.values) == [289.78904636, 284.16920838, 284.00620338]
    assert pytest.approx(ttr0.values) == [-6969528.64626286, -14032413.9597565, -9054387.41655567]

    # Now let's fix
    data1 = reader.retrieve(fix=True)  # Retrieve fixed data
    ttr1 = data1.ttr[ntime, 0, 0]
    tas1 = data1['skt'][ntime, 5, 5]
    mtntrf = data1.mtntrf[ntime, 0, 0]
    mtntrf2 = data1.mtntrf2[ntime, 0, 0]

    # Did decumulation work ?
    assert pytest.approx(ttr1.values / 3600) == [-193.92693374, -194.7589371, -159.28750829]

    # Did we get a correct derived variable specified with paramId ?
    assert pytest.approx(tas1.values) == tas0.values + 1.0

    # Did unit conversion work?
    assert pytest.approx(mtntrf.values) == [-193.92693374, -194.7589371, -159.28750829]

    # Did we get a correct derived variable specified with grib shortname ?
    assert pytest.approx(mtntrf2.values) == mtntrf.values * 2

    # Metadata checks

    # History logged
    assert 'variable derived by AQUA fixer' in tas1.attrs['history']

    # paramId and other attrs
    assert tas1.attrs['paramId'] == '235'

    assert mtntrf.attrs['paramId'] == '172179'
    assert mtntrf.attrs['units_fixed'] == 1
    assert mtntrf.attrs['units'] == 'W m**-2'

    assert mtntrf2.attrs['paramId'] == '999179'
    assert mtntrf2.attrs['units_fixed'] == 1
    assert mtntrf2.attrs['units'] == 'W m-2'  # these were coded by hand
    assert mtntrf2.attrs['long_name'] == 'Mean top net thermal radiation flux doubled'

"""Test fixer functionality for Reader"""

import pytest
import numpy as np
from aqua import Reader
from aqua.core.fixer import EvaluateFormula

LOGLEVEL = 'DEBUG'

@pytest.fixture(scope='module')
def reader_ifs_tco79_long(ifs_tco79_long_reader):
    return ifs_tco79_long_reader

@pytest.fixture(scope='module')
def data_ifs_tco79_long(ifs_tco79_long_data):
    return ifs_tco79_long_data

@pytest.fixture(scope='module')
def reader_ifs_tco79_long_fixFalse(ifs_tco79_long_fixFalse_reader):
    return ifs_tco79_long_fixFalse_reader

@pytest.fixture(scope='module')
def data_ifs_tco79_long_fixFalse(ifs_tco79_long_fixFalse_data):
    return ifs_tco79_long_fixFalse_data

@pytest.mark.aqua
def test_fixer_ifs_long(data_ifs_tco79_long, data_ifs_tco79_long_fixFalse):
    """Test basic fixing"""

    ntime = [10, 20, 1000]  # points in time to be checked (includes 1 month jump)
    data0 = data_ifs_tco79_long_fixFalse  # Retrieve not fixed data
    ttr0 = data0.ttr[ntime, 0, 0]
    tas0 = data0['2t'][ntime, 5, 5]

    # Preliminary - did we read the correct values?
    assert pytest.approx(tas0.values) == [289.78904636, 284.16920838, 284.00620338]
    assert pytest.approx(ttr0.values) == [-6969528.64626286, -14032413.9597565, -9054387.41655567]

    # Now let's fix
    data1 = data_ifs_tco79_long  # Retrieve fixed data
    # This is the decumulated ttr
    tnlwrf = data1.tnlwrf[ntime, 0, 0]
    tas1 = data1['skt'][ntime, 5, 5]
    mtntrf = data1.mtntrf[ntime, 0, 0]
    mtntrf2 = data1.mtntrf2[ntime, 0, 0]

    # Did decumulation work ?
    assert pytest.approx(tnlwrf.values) == [-193.92693374, -194.7589371, -159.28750829]

    # Did we get a correct derived variable specified with paramId ?
    assert pytest.approx(tas1.values) == tas0.values + 1.0

    # Did unit conversion work?
    assert pytest.approx(mtntrf.values) == [-193.92693374, -194.7589371, -159.28750829]

    # Did we get a correct derived variable specified with grib shortname ?
    assert pytest.approx(mtntrf2.values) == mtntrf.values * 2

    # Metadata checks

    # History logged
    assert 'Variable var235, derived with 2t+1.0 by fixer' in tas1.attrs['history']

    # paramId and other attrs
    assert tas1.attrs['paramId'] == '235'

    assert mtntrf.attrs['paramId'] == '172179'
    assert mtntrf.attrs['units_fixed'] == 1
    assert mtntrf.attrs['units'] == 'W m**-2'

    assert mtntrf2.attrs['paramId'] == '999179'
    assert mtntrf2.attrs['units_fixed'] == 1
    assert mtntrf2.attrs['units'] == 'W m-2'  # these were coded by hand
    assert mtntrf2.attrs['long_name'] == 'Mean top net thermal radiation flux doubled'


@pytest.mark.aqua
def test_fixer_ifs_long_mindate():
    """Test fixing with a minimum date functionality"""

    reader = Reader(model="IFS", exp="test-tco79", source="long-mindate",
                    fix=True, loglevel=LOGLEVEL)
    data = reader.retrieve(var='2t')

    data1 = data['2t'].sel(time="2020-07-31T00:00")[1,1]
    data2 = data['2t'].sel(time="2020-08-01T00:00")[1,1]

    assert np.isnan(data1.values)
    assert not np.isnan(data2.values)
    assert 'mindate' in data['2t'].attrs
    assert data['2t'].attrs['mindate'] == '2020-08-01T00:00'


@pytest.mark.aqua
def test_fixer_ifs_names():
    """Check with fixer_name method"""

    reader = Reader(model="IFS", exp="test-tco79", source="short_masked", loglevel=LOGLEVEL)
    data = reader.retrieve(var=['2t'])
    assert data['2t'].attrs['donald'] == 'duck'

@pytest.mark.aqua
def test_fixer_ifs_disable():
    """Check with fixer_name: False method"""

    reader = Reader(model="IFS", exp="test-tco79", source="short_disable_fix", loglevel=LOGLEVEL)
    assert reader.fix is False

@pytest.mark.aqua
def test_fixer_ifs_timeshift():
    """Check fixer for timeshift with both timestep and pandas"""

    reader = Reader(model="IFS", exp="test-tco79", source="long-shift-timestep", loglevel=LOGLEVEL)
    data = reader.retrieve()
    assert data.time[0].values == np.datetime64('2020-01-19T00:00:00')

    reader = Reader(model="IFS", exp="test-tco79", source="long-shift-pandas", loglevel=LOGLEVEL)
    data = reader.retrieve()
    assert data.time[0].values == np.datetime64('2020-01-01T00:00:00')


@pytest.mark.aqua
def test_fixer_ifs_coords():
    """Check with fixer_name and coords block"""

    reader = Reader(model="IFS", exp="test-tco79", source="short_masked-coord-test", loglevel=LOGLEVEL)
    data = reader.retrieve()
    assert 'timepippo' in data.coords
    assert 'cellspippo' in data.dims


@pytest.mark.aqua
def test_fixer_fesom_coords():
    """Check with fixer_name and coords block"""

    reader = Reader(model="FESOM", exp="test-pi", source="original_3d_coord_fix", datamodel=False, loglevel=LOGLEVEL)
    data = reader.retrieve()
    assert 'level' in data.coords
    assert 'a lot of water' in data.level.attrs['units']


@pytest.mark.aqua
def test_fixer_fesom_names():
    """Check with fixer parent from fixer_name method"""

    reader = Reader(model="FESOM", exp="test-pi", source="original_2d_fix", loglevel=LOGLEVEL)
    data = reader.retrieve()
    assert data['mlotst125'].attrs['uncle'] == 'scrooge'

@pytest.mark.aqua
def test_fixer_deltat():
    """Check that output for deltat read from metadata and from fixes are the same"""
    
    reader1 = Reader(model='IFS', exp='test-tco79', source='long-deltat', loglevel=LOGLEVEL)
    data_metadata = reader1.retrieve(var='tnlwrf').isel(time=5)
    reader2 = Reader(model='IFS', exp='test-tco79', source='long', loglevel=LOGLEVEL)
    data_fixer = reader2.retrieve(var='tnlwrf').isel(time=5)
    assert data_metadata.equals(data_fixer)
    assert reader1.fixer.deltat == 3600
    assert reader2.fixer.deltat == 3600


@pytest.fixture
def data_2t_tp(era5_hpz3_monthly_data):
    return era5_hpz3_monthly_data

@pytest.mark.aqua
class TestEvaluateFormula:
    def test_evaluate_formula(self, data_2t_tp):
        formula = "2t -273.15"
        convert = EvaluateFormula(
            data=data_2t_tp, formula=formula,
            units='Celsius',
            short_name="2t_celsius",
            long_name='2t converted to Celsius').evaluate()

        assert convert.attrs['short_name'] == '2t_celsius'
        assert convert.attrs['long_name'] == '2t converted to Celsius'
        assert convert.attrs['units'] == 'Celsius'

        original_values = (data_2t_tp['2t'].isel(time=0) - 273.15).mean()
        expected_values = convert.isel(time=0).mean()
        assert np.allclose(original_values.values, expected_values.values)

    def test_complex_formula(self, data_2t_tp):
        formula = "((2t - 273.15)^2)/2 +  (tprate / 1000)"
        convert = EvaluateFormula(
            data=data_2t_tp, formula=formula,
            units='Celsius^2 * hPa',
            short_name="complex_calc",
            long_name='Complex calculation').evaluate()

        assert convert.attrs['short_name'] == 'complex_calc'
        assert convert.attrs['long_name'] == 'Complex calculation'
        assert convert.attrs['units'] == 'Celsius^2 * hPa'

        original_values = (((data_2t_tp['2t'].isel(time=0) - 273.15)**2)/2 + (data_2t_tp['tprate'].isel(time=0) / 1000)).mean()
        expected_values = convert.isel(time=0).mean()
        assert np.allclose(original_values.values, expected_values.values)

    def test_magnitude(self, data_2t_tp):
        """Test magnitude calculation"""
        formula = "(2t^2 + 2t^2)^0.5"
        convert = EvaluateFormula(
            data=data_2t_tp, formula=formula,
            units='K',
            short_name="magnitude_2t",
            long_name='Magnitude of 2t vector').evaluate()
        
        assert convert.attrs['short_name'] == 'magnitude_2t'
        assert convert.attrs['long_name'] == 'Magnitude of 2t vector'
        assert convert.attrs['units'] == 'K'
        original_values = ((data_2t_tp['2t'].isel(time=0)**2 + data_2t_tp['2t'].isel(time=0)**2)**0.5).mean()
        expected_values = convert.isel(time=0).mean()
        assert np.allclose(original_values.values, expected_values.values)

    def test_wrong_formula(self, data_2t_tp):
        """Test wrong parentheses handling"""
        formula = "(2t - 273.15)) + (tprate / 1000"
        with pytest.raises(ValueError):
            EvaluateFormula(data=data_2t_tp, formula=formula).evaluate()
        formula = "((2t - 273.15) + (tprate / 1000)"
        with pytest.raises(ValueError):
            EvaluateFormula(data=data_2t_tp, formula=formula).evaluate()
        with pytest.raises(KeyError):
            EvaluateFormula(data=data_2t_tp, formula="2t ++ tprate").evaluate()

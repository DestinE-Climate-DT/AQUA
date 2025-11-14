import pytest 
import xarray as xr
from aqua.diagnostics import SeaIce
from aqua.exceptions import NoDataError
from conftest import APPROX_REL, LOGLEVEL

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
abs_rel = 1e-4
loglevel = LOGLEVEL

catalog = 'ci'
model = 'FESOM'
exp = 'hpz3'
source = 'monthly-2d'

# pytestmark groups tests that run sequentially on the same worker to avoid conflicts
# These tests repeatedly call SeaIce.compute_seaice() which accesses shared data
pytestmark = [
    pytest.mark.diagnostics,
    pytest.mark.xdist_group(name="diagnostic_setup_class")
]

class TestSeaIce:
    """Test the SeaIce class."""
    
    @pytest.mark.parametrize(
        ('method', 'region', 'value', 'expected_units', 'variable', 'calc_std_freq', 'expect_exception', 'error_message'),
        [
        # Valid cases without standard deviation
        ('extent', 'arctic',     17.2719, 'million km^2',   'siconc', None, None, None),
        ('extent', 'weddell_sea', 4.5872, 'million km^2',   'siconc', None, None, None),
        ('volume', 'arctic',     13.7552, 'thousands km^3', 'siconc', None, None, None),
        ('volume', 'antarctic',   9.1140, 'thousands km^3', 'siconc', None, None, None),

        # Valid cases with standard deviation computation
        ('extent', 'arctic',    3.9402, 'million km^2',   'siconc', 'annual',  None, None),
        ('extent', 'antarctic', 0.7623, 'million km^2',   'siconc', 'monthly', None, None),
        ('volume', 'antarctic', 1.003,  'thousands km^3', 'siconc', 'monthly', None, None),

        # Invalid cases (Errors expected)
        ('wrong_method', 'antarctic',   None, None, 'siconc',   None, ValueError, "Invalid method"),
        # ('extent',       'weddell_sea', None, None, 'errorvar', None, ValueError,   None),
        # ('volume',       'antarctic',   None, None, 'errorvar', None, ValueError,   None),

        # Invalid standard deviation cases
        # ('extent', 'weddell_sea', None, None, 'errorvar', None, ValueError, None),
        # ('volume', 'antarctic',   None, None, 'errorvar', None, ValueError, None)
        ]
    )
    def test_seaice_compute_with_std(self, method, region, value, expected_units, variable,
                                     calc_std_freq, expect_exception, error_message):
        """Test sea ice computation including std for both valid and invalid cases."""

        seaice = SeaIce(catalog=catalog, model=model, exp=exp, source=source, 
                        startdate="1991-01-01", enddate="2000-01-01", regions=region, regrid='r100', loglevel=loglevel)

        # Handle expected exceptions first
        if expect_exception:
            with pytest.raises(expect_exception, match=error_message if error_message else ""):
                seaice.compute_seaice(method=method, var=variable, calc_std_freq=calc_std_freq)
            return

        # Valid case: compute sea ice with or without standard deviation result is a Tuple if calc_std_freq is not None
        result = seaice.compute_seaice(method=method, var=variable, calc_std_freq=calc_std_freq)

        if calc_std_freq:

            assert isinstance(result, tuple)
            assert len(result) == 2

            # unpack the tuple 
            res, res_std = result

            assert isinstance(res, xr.Dataset)
            assert isinstance(res_std, xr.Dataset)

            regionlower = region.lower().replace(" ", "_")
            var_name = f'sea_ice_{method}_{regionlower}'
            std_var_name = f'std_sea_ice_{method}_{regionlower}'

            assert all(c in ['variable', 'time'] for c in res.coords)
            assert list(res.data_vars) == [var_name]
            assert res.attrs['units'] == expected_units

            # Adjusted assertion for time coordinate
            expected_time_coord = "year" if calc_std_freq == "annual" else "month" if calc_std_freq == "monthly" else "time"
            assert expected_time_coord in res_std.coords

            assert list(res_std.data_vars) == [std_var_name]
            assert res_std.attrs['units'] == expected_units  # Std should retain units

        else:
            # If no std computation, result should be a single Dataset
            assert isinstance(result, xr.Dataset)
            regionlower = region.lower().replace(" ", "_")
            var_name = f'sea_ice_{method}_{regionlower}'
            
            assert all(c in ['variable', 'time'] for c in result.coords)
            assert list(result.data_vars) == [var_name]
            assert result.attrs['units'] == expected_units
            assert result[var_name].values[5] == pytest.approx(value, rel=approx_rel)

    @pytest.mark.parametrize(
        ('method', 'region', 'variable', 'value', 'kwargs', 'expected_coords'),
        [
        ('extent', 'arctic',    'siconc', 21.4141, {'calc_std_freq': 'monthly', 'get_seasonal_cycle': True}, ['variable', 'month']),
        ('extent', 'antarctic', 'siconc',  3.2294, {'calc_std_freq': 'monthly', 'get_seasonal_cycle': True}, ['variable', 'month']),
        ]
    )
    def test_get_seasonal_cycle_with_region(self, method, region, variable, value, kwargs, expected_coords):
        """Test the get_seasonal_cycle functionality with region as a parameter."""

        seaice = SeaIce(catalog=catalog, model=model, exp=exp, source=source,
                        startdate="1991-01-01", enddate="2000-01-01", regions=region, 
                        regrid='r100', loglevel=loglevel)

        result, result_std = seaice.compute_seaice(method=method, var=variable, **kwargs)

        # Assertions for the seasonal cycle
        assert isinstance(result, xr.Dataset)
        assert isinstance(result_std, xr.Dataset)
        assert 'month' in result.coords
        assert 'month' in result_std.coords

        regionlower = region.lower().replace(" ", "_")
        var_name = f'sea_ice_{method}_{regionlower}'
        assert result[var_name].values[0] == pytest.approx(value, rel=approx_rel)

    ################## TO be implemented ###################
    @pytest.mark.parametrize(
        ('method', 'region',  'value', 'variable', 'expect_exception'),
        [
        # Valid cases without standard deviation
        ('fraction',  'arctic',    0.2817,  'siconc',   None),
        ('thickness', 'antarctic', 0.0619,  'sithick',  None),

        # Invalid cases (Errors expected)
        ('fraction',[1,2,3], None, 'siconc', ValueError),
        # ('wrong_method', 'antarctic',   None, None, 'siconc',   None, ValueError, "Invalid method"),
        # ('fraction',     'weddell_sea', None, None, 'errorvar', None, KeyError, None),
        # ('thickness',    'antarctic',   None, None, 'errorvar', None, KeyError, None),

        # # Invalid standard deviation cases
        # ('extent', 'weddell_sea', None, None, 'errorvar', None, KeyError, None),
        # ('volume', 'antarctic',   None, None, 'errorvar', None, KeyError, None)
        ]
    )
    def test_seaice_2d_monthly_climatology(self, method, region, value, variable, expect_exception):
        """Test sea ice computation for 2D monthly climatology including std for both valid and invalid cases."""

        def create_seaice():
            return SeaIce(catalog=catalog, model=model, exp=exp, source=source,
                startdate="1991-01-01", enddate="2000-01-01", regions=region,
                regrid='r100', loglevel=loglevel)

        # Handle expected exceptions first
        if expect_exception:
            with pytest.raises(expect_exception):
                seaice = create_seaice()
                seaice.compute_seaice(method=method, var=variable)
            return
        else:
            seaice = create_seaice()
        
        # Valid case: compute sea ice
        result = seaice.compute_seaice(method=method, var=variable)

        assert isinstance(result, xr.Dataset)

        # Get the specific variable from the dataset
        regionlower = region.lower().replace(" ", "_")
        var_name = f'sea_ice_{method}_{regionlower}'
        result_data = result[var_name]

        assert isinstance(result_data, xr.DataArray)

        meanres = result_data.mean(skipna=True).values

        assert meanres == pytest.approx(value, rel=approx_rel, abs=abs_rel)

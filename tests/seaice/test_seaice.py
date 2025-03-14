import pytest 
import xarray as xr
from aqua.diagnostics import SeaIce

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

@pytest.mark.diagnostics
class TestSeaIce:
    """Test the SeaIce class."""
    
    @pytest.mark.parametrize(
        ('method', 'region', 'value', 'expected_units', 'variable', 'calc_std_freq', 'expect_exception', 'error_message'),

        [
        # Valid cases without standard deviation
        ('extent', 'Arctic',     18.4750, 'million km^2', 'siconc', None, None, None),
        ('extent', 'Weddell Sea', 4.7523, 'million km^2', 'siconc', None, None, None),
        ('volume', 'Arctic',    14.6634, 'thousands km^3', 'siconc', None, None, None),
        ('volume', 'Antarctic', 9.7129, 'thousands km^3', 'siconc', None, None, None),

        # Valid cases with standard deviation computation
        ('extent', 'Arctic',  3.9402, 'million km^2',   'siconc', 'annual',  None, None),
        ('extent', 'Antarctic', 0.7623, 'million km^2', 'siconc', 'monthly', None, None),
        ('volume', 'Antarctic', 1.003, 'thousands km^3', 'siconc', 'monthly', None, None),
        # Invalid cases (Errors expected)
        ('wrong_method', 'Antarctic', None, None, 'siconc', None, ValueError, "Invalid method"),
        ('extent', 'Weddell Sea', None, None, 'errorvar', None, KeyError, None),
        ('volume', 'Antarctic', None, None, 'errorvar', None, KeyError, None),

        # Invalid standard deviation cases
        ('extent', 'Weddell Sea', None, None, 'errorvar', None, KeyError, None),
        ('volume', 'Antarctic',   None, None, 'errorvar', None, KeyError, None)
        ]
    )
    def test_seaice_compute_with_std(self, method, region, value, expected_units, variable,
                            calc_std_freq, expect_exception, error_message):
        """Test sea ice computation including std for both valid and invalid cases."""

        seaice = SeaIce(catalog='ci', model='FESOM', exp='hpz3', source='monthly-2d',
                        startdate="1991-01-01", enddate="2000-01-01", regions=region, 
                        regrid='r100', loglevel=loglevel)

        # Handle expected exceptions first
        if expect_exception:
            with pytest.raises(expect_exception, match=error_message if error_message else ""):
                if method == 'extent':
                    seaice.compute_seaice(method=method, var=variable, threshold=0.15, calc_std_freq=calc_std_freq)
                else:
                    seaice.compute_seaice(method=method, var=variable, calc_std_freq=calc_std_freq)
            return

        # Valid case: compute sea ice with or without standard deviation
        # result is a Tuple if calc_std_freq is not None
        if method == 'extent':
            result = seaice.compute_seaice(method=method, var=variable, threshold=0.15, calc_std_freq=calc_std_freq)
        else:
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

            assert list(res.coords) == ['time']
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
            
            assert list(result.coords) == ['time']
            assert list(result.data_vars) == [var_name]
            assert result.attrs['units'] == expected_units
            assert result[var_name].values[5] == pytest.approx(value, rel=approx_rel)
    
    @pytest.mark.parametrize(
        ('regions_definition', 'expect_exception', 'error_message', 'expected_output'),
        [
            # Valid case: regions_definition is set
            (
             {"Arctic": "some definition", "Antarctic": "another definition"}, 
             None, None, 
             {"Arctic": "some definition", "Antarctic": "another definition"}
             ),

            # Invalid cases
            (None, ValueError, "No regions_definition found.", None),  # regions_definition is None
        ]
    )
    def test_show_regions(self, regions_definition, expect_exception, error_message, expected_output):
        """Test the show_regions method."""
        
        # Create instance of SeaIce with the necessary attributes
        seaice = SeaIce(catalog='ci', model='FESOM', exp='hpz3', source='monthly-2d',
                        startdate="1991-01-01", enddate="2000-01-01", regions="Arctic", 
                        regrid='r100', loglevel=loglevel)

        # Set regions_definition dynamically
        if regions_definition is not None:
            seaice.regions_definition = regions_definition
        else:
            if hasattr(seaice, 'regions_definition'):
                delattr(seaice, 'regions_definition')  # Remove attribute to simulate missing case

        # Handle expected exceptions
        if expect_exception:
            with pytest.raises(expect_exception, match=error_message if error_message else ""):
                seaice.show_regions()
            return

        # If no exception, verify correct output
        result = seaice.show_regions()
        assert isinstance(result, dict)
        assert result == expected_output
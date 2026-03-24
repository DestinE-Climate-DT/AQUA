"""Tests for string unit conversion utilities"""

import pytest
import string
from aqua.core.util.string import unit_to_latex, generate_random_string
from aqua.core.util.string import get_quarter_anchor_month, clean_filename

@pytest.mark.aqua
@pytest.mark.parametrize("input_str, expected", [
    # Basic cases
    ("kg", "kg"),
    
    # Simple exponents (explicit and implicit)
    ("m^2", "m$^{2}$"),
    ("m**2", "m$^{2}$"),
    ("m2", "m$^{2}$"),
    ("m-2", "m$^{-2}$"),
    ("m^-2", "m$^{-2}$"),
    ("m**-2", "m$^{-2}$"),
    
    # Division notation
    ("W/m^2", "W m$^{-2}$"),
    ("m/s", "m s$^{-1}$"),
    ("kg/m/s", "kg m$^{-1}$ s$^{-1}$"),
    ("J/kg/K", "J kg$^{-1}$ K$^{-1}$"),
    
    # Grouped division
    ("W/(m^2 s)", "W m$^{-2}$ s$^{-1}$"),
    ("kg/(m s^2)", "kg m$^{-1}$ s$^{-2}$"),
    
    # Mixed notation
    ("kg m-1 s-1", "kg m$^{-1}$ s$^{-1}$"),
    ("kg m^-1 s^-1", "kg m$^{-1}$ s$^{-1}$"),
    ("kg m**-1 s**-1", "kg m$^{-1}$ s$^{-1}$"),
    
    # Special characters
    ("°C", "°C"),
    ("µg m^-3", "µg m$^{-3}$"),
    ("µg/m^3", "µg m$^{-3}$"),
    ("%", r"$\%$"),
    
    # Dimensionless units
    ("1", "1"),
    ("1 ", "1"),
    (" 1", "1"),

    # Edge cases
    ("", ""),
    (None, None),
    ("   ", ""),
    ("m/s/s", "m s$^{-1}$ s$^{-1}$"),
    
    # Already LaTeX (should be preserved)
    ("$\\mathrm{km}^2$", "$\\mathrm{km}^2$"),
    ("10^6 $\\mathrm{km}^2$", "10^6 $\\mathrm{km}^2$"),
    (r"$\mathrm{W} \mathrm{m}^{-2}$", r"$\mathrm{W} \mathrm{m}^{-2}$"),
    ("m^{2}", "m^{2}"), # Partial LaTeX
])
def test_unit_to_latex(input_str, expected):
    """Test unit_to_latex with various input formats"""
    assert unit_to_latex(input_str) == expected

@pytest.mark.aqua
def test_unit_to_latex_complex():
    """Test more complex combinations"""
    # Test complex mixed format
    assert unit_to_latex("W m-2 K-1") == "W m$^{-2}$ K$^{-1}$"
    # Test complex division with implicit exponents
    assert unit_to_latex("kg/m3") == "kg m$^{-3}$"
    # Test with extra spaces
    assert unit_to_latex(" W /  m^2 ") == "W m$^{-2}$"


@pytest.mark.aqua
def test_generate_random_string():
    """Test generate_random_string function"""
    # Test different lengths
    for length in [0, 1, 5, 10, 20]:
        result = generate_random_string(length)
        assert len(result) == length
        # Check that all characters are letters or digits
        assert all(c in string.ascii_letters + string.digits for c in result)


@pytest.mark.aqua
@pytest.mark.parametrize("freq_string, expected", [
    ("QE-DEC", "DEC"),  # Extract month when dash present
    ("Q-MAR", "MAR"),   # Different month
    ("QS", "DEC"),      # Default when no dash
])
def test_get_quarter_anchor_month(freq_string, expected):
    """Test get_quarter_anchor_month function"""
    assert get_quarter_anchor_month(freq_string) == expected


@pytest.mark.aqua
@pytest.mark.parametrize("filename, expected", [
    ("My File Name", "my_file_name"),  # Mixed case with spaces
    ("UPPER CASE", "upper_case"),      # Uppercase with spaces
    ("file_with_spaces.txt", "file_with_spaces.txt"), # Already clean
    ("  leading spaces  ", "__leading_spaces__"),     # Edge case: leading/trailing spaces
    ("", ""),
])
def test_clean_filename(filename, expected):
    """Test clean_filename function"""
    assert clean_filename(filename) == expected

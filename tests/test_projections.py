"""Test for get_projection functions"""

import pytest
import cartopy.crs as ccrs
from aqua.core.util import get_projection

# List of (projection name, expected class, required kwargs if any)
projection_cases = [
    {"name": "plate_carree", "expected_cls": ccrs.PlateCarree, "kwargs": {}},
    {"name": "mollweide", "expected_cls": ccrs.Mollweide, "kwargs": {}},
    {"name": "orthographic", "expected_cls": ccrs.Orthographic, "kwargs": {"central_longitude": 70, "central_latitude": 0}},
    {"name": "lambert_conformal", "expected_cls": ccrs.LambertConformal, "kwargs": {"central_longitude": -10}},
    {"name": "robinson", "expected_cls": ccrs.Robinson, "kwargs": {}},
    {"name": "lambert_equal_area", "expected_cls": ccrs.LambertAzimuthalEqualArea, "kwargs": {"central_longitude": 0}},
    {"name": "rotated_pole", "expected_cls": ccrs.RotatedPole, "kwargs": {"pole_longitude": 180, "pole_latitude": 45}}
]

@pytest.mark.aqua
class TestProjections:
    """Tests for the `get_projection` utility function."""

    @pytest.mark.parametrize("case", projection_cases)
    def test_valid_projection_instantiation(self, case):
        """Test that projections are correctly instantiated from their names."""
        proj = get_projection(case["name"], **case["kwargs"])
        assert isinstance(proj, case["expected_cls"]), f"Expected {case['expected_cls']}, got {type(proj)}"

    def test_invalid_projection_name(self):
        """Test that invalid projection names raise a ValueError."""
        with pytest.raises(ValueError):
            get_projection("invalid_proj_name")

    def test_case_insensitive_lookup(self):
        """Ensure projection names are case-insensitive."""
        proj = get_projection("MoLLWeiDE")
        assert isinstance(proj, ccrs.Mollweide)

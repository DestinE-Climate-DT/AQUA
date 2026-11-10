"""Test for get_projection functions"""

import cartopy.crs as ccrs
import pytest

from aqua.core.util import get_projection
from aqua.core.util.projections import projections_maps

pytestmark = pytest.mark.aqua


@pytest.mark.parametrize("name, expected_cls", projections_maps.items(), ids=list(projections_maps))
def test_projection_instantiation(name, expected_cls):
    """Every supported name gives an instance of its cartopy class with the default arguments."""
    assert type(get_projection(name)) is expected_cls


@pytest.mark.parametrize(
    "name, kwargs, expected_params",
    [
        ("orthographic", {"central_longitude": 70, "central_latitude": 10}, {"proj": "ortho", "lon_0": 70, "lat_0": 10}),
        ("lambert_conformal", {"central_longitude": -10}, {"proj": "lcc", "lon_0": -10}),
        ("rotated_pole", {"pole_longitude": 180, "pole_latitude": 45}, {"proj": "ob_tran", "o_lat_p": 45, "lon_0": 360}),
    ],
    ids=["orthographic", "lambert_conformal", "rotated_pole"],
)
def test_projection_kwargs_forwarded(name, kwargs, expected_params):
    """Keyword arguments reach the cartopy class: fails if they are dropped (the defaults give other parameters)."""
    params = get_projection(name, **kwargs).proj4_params
    assert {key: params[key] for key in expected_params} == expected_params


def test_invalid_projection_name():
    """Test that invalid projection names raise a ValueError."""
    with pytest.raises(ValueError, match="Unsupported projection: 'invalid_proj_name'"):
        get_projection("invalid_proj_name")


def test_case_insensitive_lookup():
    """Ensure projection names are case-insensitive."""
    assert type(get_projection("MoLLWeiDE")) is ccrs.Mollweide

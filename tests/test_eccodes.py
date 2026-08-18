"""Tests for ecCodes utilities."""

import pytest

from aqua.core.exceptions import NoEcCodesShortNameError
from aqua.core.util import get_eccodes_attr


@pytest.mark.aqua
@pytest.mark.parametrize(
    "query, expected_attr",
    [
        (
            235288,
            {
                "paramId": 235288,
                "long_name": "Time-mean total cloud cover",
                "units": "%",
                "shortName": "avg_tcc",
                "cfVarName": "avg_tcc",
            },
        ),
        (
            "avg_tcc",
            {
                "paramId": "235288",
                "long_name": "Time-mean total cloud cover",
                "units": "%",
                "shortName": "avg_tcc",
                "cfVarName": "avg_tcc",
            },
        ),
        (
            164,
            {
                "paramId": 164,
                "long_name": "Total cloud cover",
                "units": "(0 - 1)",
                "shortName": "tcc",
                "cfVarName": "tcc",
            },
        ),
        (
            "tcc",
            {
                "paramId": "228164",
                "long_name": "Total Cloud Cover",
                "units": "%",
                "shortName": "tcc",
                "cfVarName": "tcc",
            },
        ),
        (
            228164,
            {
                "paramId": 228164,
                "long_name": "Total Cloud Cover",
                "units": "%",
                "shortName": "tcc",
                "cfVarName": "tcc",
            },
        ),
        (
            "issrd",
            {
                "paramId": "72",
                "long_name": "Instantaneous surface solar radiation downwards",
                "units": "W m**-2",
                "shortName": "issrd",
                "cfVarName": "issrd",
            },
        ),
        (
            72,
            {
                "paramId": 72,
                "long_name": "Instantaneous surface solar radiation downwards",
                "units": "W m**-2",
                "shortName": "issrd",
                "cfVarName": "issrd",
            },
        ),
    ],
)
def test_get_eccodes_attr_examples(query, expected_attr):
    """Test get_eccodes_attr against expected parameter attributes."""
    result = get_eccodes_attr(query, loglevel="debug")
    assert result == expected_attr
    for key, value in expected_attr.items():
        assert result[key] == value


@pytest.mark.aqua
@pytest.mark.parametrize(
    "pid, expected_short_name",
    [
        (235288, "avg_tcc"),
        (164, "tcc"),
        (228164, "tcc"),
        (72, "issrd"),
    ],
)
def test_get_eccodes_attr_param_to_shortname(pid, expected_short_name):
    """Test looking up shortName from paramId."""
    res = get_eccodes_attr(pid, loglevel="debug")
    assert res["shortName"] == expected_short_name


@pytest.mark.aqua
@pytest.mark.parametrize(
    "sn, expected_pid",
    [
        ("avg_tcc", "235288"),
        ("tcc", "228164"),
        ("issrd", "72"),
    ],
)
def test_get_eccodes_attr_shortname_to_param(sn, expected_pid):
    """Test looking up paramId from shortName."""
    res = get_eccodes_attr(sn, loglevel="debug")
    assert str(res["paramId"]) == expected_pid


@pytest.mark.aqua
@pytest.mark.parametrize(
    "var_str, expected_short_name",
    [
        ("var235288", "avg_tcc"),
        ("var72", "issrd"),
        ("var164", "tcc"),
    ],
)
def test_get_eccodes_attr_var_prefix(var_str, expected_short_name):
    """Test string paramId with 'var' prefix."""
    res = get_eccodes_attr(var_str, loglevel="debug")
    assert res["shortName"] == expected_short_name


@pytest.mark.aqua
@pytest.mark.parametrize("invalid_query", ["nonexistent_short_name_xyz", 99999999])
def test_get_eccodes_attr_not_found(invalid_query):
    """Test that NoEcCodesShortNameError is raised for invalid paramId or shortName."""
    with pytest.raises(NoEcCodesShortNameError):
        get_eccodes_attr(invalid_query, loglevel="debug")

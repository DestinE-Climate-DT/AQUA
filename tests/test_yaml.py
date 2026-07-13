"""Tests for the strict vs base mode of load_yaml jinja rendering."""

import pytest
from jinja2 import UndefinedError

from aqua.core.util import load_yaml

pytestmark = pytest.mark.aqua


def _write_template(tmp_path):
    """Write a tiny yaml template that references a single jinja variable."""
    cfg = tmp_path / "template.yaml"
    cfg.write_text("key: '{{ missing }}'\n")
    return str(cfg)


def test_load_yaml_strict_raises_on_missing_var(tmp_path):
    """strict=True must raise UndefinedError when a referenced var is absent."""
    cfg = _write_template(tmp_path)
    with pytest.raises(UndefinedError):
        load_yaml(cfg, definitions={"other": "x"}, strict=True)


def test_load_yaml_lenient_blanks_missing_var(tmp_path):
    """Default (lenient) mode renders a missing var as empty string."""
    cfg = _write_template(tmp_path)
    result = load_yaml(cfg, definitions={"other": "x"})
    assert result["key"] == ""

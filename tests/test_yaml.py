"""Tests for the strict vs base mode of load_yaml jinja rendering."""

import os

import pytest
from jinja2 import UndefinedError

import aqua.core
from aqua.core.util import load_multi_yaml, load_yaml

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


def _write_grids(tmp_path, files):
    for filename, names in files.items():
        entries = "".join(f"  {name}:\n    space_coord: [lon, lat]\n" for name in names)
        (tmp_path / filename).write_text(f"grids:\n{entries}")


def test_load_multi_yaml_check_duplicates_raises(tmp_path):
    """The same entry in two files of a folder is an error when check_duplicates is set."""
    _write_grids(tmp_path, {"a.yaml": ["g1", "g2"], "b.yaml": ["g2", "g3"]})
    with pytest.raises(ValueError, match="g2"):
        load_multi_yaml(folder_path=str(tmp_path), check_duplicates=True)


def test_load_multi_yaml_duplicates_allowed_by_default(tmp_path):
    """Without check_duplicates the files are merged silently."""
    _write_grids(tmp_path, {"a.yaml": ["g1", "g2"], "b.yaml": ["g2", "g3"]})
    assert set(load_multi_yaml(folder_path=str(tmp_path))["grids"]) == {"g1", "g2", "g3"}


def test_load_multi_yaml_check_duplicates_ok(tmp_path):
    """Distinct entries pass the check."""
    _write_grids(tmp_path, {"a.yaml": ["g1"], "b.yaml": ["g2"]})
    assert set(load_multi_yaml(folder_path=str(tmp_path), check_duplicates=True)["grids"]) == {"g1", "g2"}


def test_packaged_grids_have_no_duplicates():
    """Grid names must be unique across the files shipped in aqua/core/config/grids."""
    folder = os.path.join(os.path.dirname(aqua.core.__file__), "config", "grids")
    assert load_multi_yaml(folder_path=folder, check_duplicates=True)["grids"]

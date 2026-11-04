"""Guard: coordinate names must come from AquaCoordNames, not be hard-coded in aqua/core."""

import ast
from pathlib import Path

import pytest

import aqua.core

CORE = Path(aqua.core.__file__).parent

# Names of the data model coordinates which must not be hard-coded, as string, keyword argument or attribute.
# lat/lon are free as strings and keywords: they are user-facing arguments (select_area(lon=..., lat=...))
STRINGS = {"time", "plev"}
KEYWORDS = {"time"}
ATTRIBUTES = {"time", "lat", "lon"}

# Code working on raw data before any data model exists, or not using data model coordinates at all
EXCLUDED = (
    "console",  # CLI
    "catgen",  # catalog generator, FDB keys
    "intake_drivers",  # raw FDB/GSV keys
    "default",  # the definitions themselves
    "data_model",  # the definitions themselves
    "gridbuilder",  # works on native model data
    "analysis",  # log records
    "lock",
)

# file -> hard-coded names tolerated, with the reason
ALLOWED = {
    "util/graphics.py": {"lat", "lon"},  # healpix_resample output grid and add_cyclic_lon, regridded data
    "graphics/lat_lon_profiles.py": {"lat", "lon"},  # coordinate guessed with coord_names()
    "reader/reader.py": {"time"},  # chunks keys documented to users
}


def _is_coords(node):
    """True for `coords`, `self.coords` and `AquaCoordNames(...)`: the AquaCoordNames accessors."""
    if isinstance(node, ast.Call):
        return isinstance(node.func, ast.Name) and node.func.id == "AquaCoordNames"
    return (isinstance(node, ast.Name) and node.id == "coords") or (isinstance(node, ast.Attribute) and node.attr == "coords")


def _hardcoded(path):
    tree = ast.parse(path.read_text())
    found = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Constant) and isinstance(node.value, str) and node.value in STRINGS:
            found.append((node.lineno, node.value))
        elif isinstance(node, ast.keyword) and node.arg in KEYWORDS:
            found.append((node.value.lineno, node.arg))
        elif isinstance(node, ast.Attribute) and node.attr in ATTRIBUTES and not _is_coords(node.value):
            if not (isinstance(node.value, ast.Name) and node.value.id in {"time", "np", "pd", "datetime"}):
                found.append((node.lineno, node.attr))
    return found


def _files():
    for path in sorted(CORE.rglob("*.py")):
        rel = path.relative_to(CORE).as_posix()
        if rel.split("/")[0] in EXCLUDED:
            continue
        yield rel, path


@pytest.mark.aqua
@pytest.mark.parametrize("rel,path", list(_files()), ids=lambda x: x if isinstance(x, str) else "")
def test_no_hardcoded_coords(rel, path):
    allowed = ALLOWED.get(rel, set())
    bad = [(line, name) for line, name in _hardcoded(path) if name not in allowed]
    assert not bad, f"{rel} hard-codes coordinate names (use AquaCoordNames): {bad}"

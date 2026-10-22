"""Tests for the consistency of the grid definitions in aqua/core/config/grids."""

import os
from collections import defaultdict

import pytest

import aqua.core
from aqua.core.util import load_yaml

pytestmark = pytest.mark.aqua

GRIDS_FOLDER = os.path.join(os.path.dirname(aqua.core.__file__), "config", "grids")


def test_grid_names_are_unique():
    """Each grid name must be defined in a single file, since files are merged in arbitrary order."""
    definitions = defaultdict(list)
    for filename in sorted(os.listdir(GRIDS_FOLDER)):
        if filename.endswith((".yml", ".yaml")):
            grids = load_yaml(os.path.join(GRIDS_FOLDER, filename), definitions={"grids": "grids"}).get("grids", {})
            for name in grids:
                definitions[name].append(filename)

    duplicates = {name: files for name, files in definitions.items() if len(files) > 1}
    assert not duplicates, f"Grid names defined in more than one file: {duplicates}"

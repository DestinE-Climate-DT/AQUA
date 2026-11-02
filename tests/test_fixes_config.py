"""Tests for the consistency of the fixes in aqua/core/config/fixes."""

import os
from collections import defaultdict

import pytest

import aqua.core
from aqua.core.util import load_yaml

pytestmark = pytest.mark.aqua

FIXES_FOLDER = os.path.join(os.path.dirname(aqua.core.__file__), "config", "fixes")


def _load_fixer_names():
    """Return a dictionary {fixer_name: [files defining it]} and the merged fixer_name block."""
    definitions = defaultdict(list)
    fixes = {}
    for filename in sorted(os.listdir(FIXES_FOLDER)):
        if filename.endswith((".yml", ".yaml")):
            block = load_yaml(os.path.join(FIXES_FOLDER, filename)).get("fixer_name", {})
            for name, fix in block.items():
                definitions[name].append(filename)
                fixes[name] = fix
    return definitions, fixes


def test_fixer_names_are_unique():
    """Each fixer name must be defined in a single file, since files are merged in arbitrary order."""
    definitions, _ = _load_fixer_names()
    duplicates = {name: files for name, files in definitions.items() if len(files) > 1}
    assert not duplicates, f"Fixer names defined in more than one file: {duplicates}"


def test_fixer_parents_exist():
    """Each parent must be an existing fixer without a parent (only one level of inheritance is resolved)."""
    _, fixes = _load_fixer_names()
    for name, fix in fixes.items():
        parent = fix.get("parent")
        if parent is not None:
            assert parent in fixes, f"Parent {parent} of {name} not found"
            assert "parent" not in fixes[parent], f"Parent {parent} of {name} has itself a parent"

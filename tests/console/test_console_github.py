"""The one console test that talks to GitHub: every other console test serves the catalog repository locally."""

import os

import pytest

pytestmark = [pytest.mark.console, pytest.mark.network]


def test_console_github_catalog(tmp_path, set_home, run_aqua, capfd):
    """Add, list as available, update and remove the `ci` catalog from the real Climate-DT-catalog repository."""
    mydir = str(tmp_path)
    set_home(mydir)
    run_aqua(["install", "github"])

    run_aqua(["add", "ci"])
    catalog = os.path.join(mydir, ".aqua", "catalogs", "ci")
    assert os.path.isfile(os.path.join(catalog, "catalog.yaml"))
    assert os.path.isfile(os.path.join(catalog, "machine.yaml"))

    capfd.readouterr()
    run_aqua(["avail"])
    out, _ = capfd.readouterr()
    assert "'ci'" in out

    run_aqua(["-v", "update", "-c", "ci"])
    assert os.path.isfile(os.path.join(catalog, "catalog.yaml"))

    run_aqua(["remove", "ci"])
    assert not os.path.exists(catalog)

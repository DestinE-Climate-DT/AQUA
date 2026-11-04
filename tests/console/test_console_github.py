"""The one console test that talks to GitHub: every other console test serves the catalog repository locally."""

import os

import pytest

pytestmark = [pytest.mark.console, pytest.mark.network]


def test_console_github_catalog(tmp_path, set_home, run_aqua, capfd):
    """Add, list as available, update and remove `ci` from the real Climate-DT-catalog repository; add a missing catalog."""
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

    # a catalog that is not in the repository: the fetcher reports the available ones and the console exits
    with pytest.raises(SystemExit) as excinfo:
        run_aqua(["add", "nonexistent-catalog-test-xyz"])
    assert excinfo.value.code == 1
    assert not os.path.exists(os.path.join(mydir, ".aqua", "catalogs", "nonexistent-catalog-test-xyz"))

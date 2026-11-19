"""Tests of the `aqua project` noun on minimal installations built under tmp_path."""

import logging

import pytest

from aqua.core.util import load_yaml

pytestmark = pytest.mark.console


def test_project_set_and_list(project_install, run_aqua, capfd):
    """Fails if `project set` does not write the key or `project list` misses a project, its mode or the active mark."""
    configfile = project_install()

    run_aqua(["project", "set", "claima"])
    assert load_yaml(configfile)["project"] == "claima"

    capfd.readouterr()
    run_aqua(["project", "list"])
    out, _ = capfd.readouterr()
    # the package version depends on the environment: aqua-claima may be installed or not
    lines = out.splitlines()[1:]
    assert [line.split(", aqua-")[0] for line in lines] == ["* claima (standard", "  diagnostics (standard"]


@pytest.mark.parametrize(
    "command, message",
    [
        (["project", "set", "climatedt"], "Project climatedt is not installed: none of analysis, collections, tools"),
        (["project", "set", "core"], "core is not a project"),
        (["project", "install", "core"], "core is not a project"),
        (["project", "update", "core"], "core is not a project"),
        (["project", "remove", "core"], "core is not a project"),
        (["catalog", "set", "claima"], "claima catalog is not installed!"),
    ],
)
def test_project_errors(project_install, run_aqua, aqua_caplog, command, message):
    """Fails if a missing project or core can be set, or a project name is accepted as a catalog."""
    configfile = project_install()
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any(r.getMessage().startswith(message) for r in caplog.records)
    assert load_yaml(configfile)["project"] == "diagnostics"


@pytest.mark.parametrize("command", [["project", "set", "diagnostics"], ["project", "list"]])
def test_project_verbs_refuse_pre_project_installation(project_install, run_aqua, aqua_caplog, command):
    """Fails if the project verbs run on an installation without project key instead of pointing to `aqua install`."""
    configfile = project_install(project=None)
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any("predates project support" in r.getMessage() and "aqua install" in r.getMessage() for r in caplog.records)
    assert "project" not in load_yaml(configfile)

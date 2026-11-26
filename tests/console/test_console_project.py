"""Tests of the `aqua project` noun on minimal installations built under tmp_path."""

import logging

import pytest

from aqua.core.util import load_yaml

pytestmark = pytest.mark.console


def test_project_set_and_list(project_install, run_aqua, capfd):
    """Fails if `project set` does not move the project to the top of the installed ones, or `project list` misses a
    project or its mode, or does not list the projects in alphabetical order."""
    configfile = project_install()

    run_aqua(["project", "set", "claima"])
    assert load_yaml(configfile)["project"] == ["claima", "diagnostics"]

    capfd.readouterr()
    run_aqua(["project", "list"])
    out, _ = capfd.readouterr()
    # the package version depends on the environment: aqua-claima may be installed or not
    lines = out.splitlines()[1:]
    assert [line.split(", aqua-")[0] for line in lines] == ["\t - claima (standard", "\t - diagnostics (standard"]


@pytest.mark.parametrize(
    "command, message",
    [
        (["project", "set", "climatedt"], "Project climatedt is not installed: add it with `aqua project install climatedt`"),
        (["project", "set", "core"], "core is not a project"),
        (["project", "install", "core"], "core is not a project"),
        (["project", "update", "core"], "core is not a project"),
        (["project", "remove", "core"], "core is not a project"),
        (["catalog", "set", "claima"], "claima catalog is not installed!"),
    ],
)
def test_project_errors(project_install, run_aqua, aqua_caplog, command, message):
    """Fails if a project that is not listed or core can be set, or a project name is accepted as a catalog."""
    configfile = project_install()
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any(r.getMessage().startswith(message) for r in caplog.records)
    assert load_yaml(configfile)["project"] == ["diagnostics", "claima"]


@pytest.mark.parametrize("command", [["project", "set", "diagnostics"], ["project", "list"]])
def test_project_verbs_refuse_pre_project_installation(project_install, run_aqua, aqua_caplog, command):
    """Fails if the project verbs run on an installation without project key instead of pointing to `aqua install`."""
    configfile = project_install(project=None)
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any("predates project support" in r.getMessage() and "aqua install" in r.getMessage() for r in caplog.records)
    assert "project" not in load_yaml(configfile)

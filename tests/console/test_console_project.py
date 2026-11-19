"""Tests of the `aqua project` noun on minimal installations built under tmp_path."""

import logging
import os

import pytest

from aqua.core.util import dump_yaml, load_yaml

pytestmark = pytest.mark.console


@pytest.fixture
def project_install(tmp_path, monkeypatch):
    """Return a function building an installation with projects `diagnostics` (active) and `claima`.

    The installation is found through AQUA_CONFIG; `project=None` writes no project key (pre-project layout).
    """
    monkeypatch.delenv("AQUA_PROJECT", raising=False)

    def _build(project="diagnostics"):
        config = {"machine": "ci", "catalog": None}
        if project is not None:
            config["project"] = project
        dump_yaml(os.path.join(tmp_path, "config-aqua.yaml"), config)
        for folder in ("analysis/diagnostics", "tools/diagnostics", "collections/claima", "catalogs"):
            os.makedirs(os.path.join(tmp_path, folder), exist_ok=True)
        monkeypatch.setenv("AQUA_CONFIG", str(tmp_path))
        return os.path.join(tmp_path, "config-aqua.yaml")

    return _build


def test_project_set_and_list(project_install, run_aqua, capfd):
    """Fails if `project set` does not write the key or `project list` misses a project or the active mark."""
    configfile = project_install()

    run_aqua(["project", "set", "claima"])
    assert load_yaml(configfile)["project"] == "claima"

    capfd.readouterr()
    run_aqua(["project", "list"])
    out, _ = capfd.readouterr()
    assert out.splitlines()[1:] == ["* claima", "  diagnostics"]


@pytest.mark.parametrize(
    "command, message",
    [
        (["project", "set", "climatedt"], "Project climatedt is not installed: none of analysis, collections, tools"),
        (["project", "set", "core"], "core is not a project"),
        (["project", "install", "core"], "core is not a project"),
        (["catalog", "set", "claima"], "claima is a project, not a catalog: use `aqua project set claima`"),
    ],
)
def test_project_errors(project_install, run_aqua, aqua_caplog, command, message):
    """Fails if a missing project or core can be set, or `aqua catalog set <project>` does not point to `project set`."""
    configfile = project_install()
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any(r.getMessage().startswith(message) for r in caplog.records)
    assert load_yaml(configfile)["project"] == "diagnostics"


@pytest.mark.parametrize("verb", ["install", "update", "remove"])
def test_project_verbs_not_implemented(project_install, run_aqua, verb):
    """Fails if a project verb not implemented yet does something instead of raising."""
    project_install()

    with pytest.raises(NotImplementedError, match=f"`aqua project {verb}` is available in a later PR"):
        run_aqua(["project", verb, "claima"])


@pytest.mark.parametrize("command", [["project", "set", "diagnostics"], ["project", "list"]])
def test_project_verbs_refuse_pre_project_installation(project_install, run_aqua, aqua_caplog, command):
    """Fails if the project verbs run on an installation without project key instead of pointing to `aqua install`."""
    configfile = project_install(project=None)
    caplog = aqua_caplog(logging.getLogger("AQUA"))

    with pytest.raises(SystemExit):
        run_aqua(command)

    assert any("predates project support" in r.getMessage() and "aqua install" in r.getMessage() for r in caplog.records)
    assert "project" not in load_yaml(configfile)

"""Integration tests for the multi-plugin AQUA install (discover_aqua_components + install/update).

A fake 'mockplugin' aqua.plugins entry point is registered purely in-process (no pip install,
no separate repo), mirroring the real aqua-diagnostics contract:
    [project.entry-points."aqua.plugins"]
    diagnostics = "aqua.diagnostics:get_install_dirs"
"""

import os
from importlib.metadata import EntryPoint
from importlib.metadata import entry_points as real_entry_points

import pytest

# reuse existing CLI test helpers instead of duplicating them, re-exported under their
# original fixture names so pytest resolves scopes (e.g. tmpdir) exactly as in test_console.py
from test_console import MACHINE
from test_console import run_aqua as _run_aqua
from test_console import run_aqua_console_with_input as _run_aqua_console_with_input
from test_console import set_home as _set_home
from test_console import tmpdir as _tmpdir

import aqua
from aqua.core.console import components as components_module
from aqua.core.console.components import discover_aqua_components

run_aqua = _run_aqua
run_aqua_console_with_input = _run_aqua_console_with_input
set_home = _set_home
tmpdir = _tmpdir

FIXTURE_ROOT = os.path.join(os.path.dirname(__file__), "fixtures", "mockplugin")

# "mockplugin" resolves to a real fixture package; "brokenplugin" points to a module that
# does not exist on disk, so ep.load() raises and the component is reported as not installed.
MOCK_ENTRY_POINTS = [
    EntryPoint(name="mockplugin", value="aqua.mockplugin:get_install_dirs", group="aqua.plugins"),
    EntryPoint(name="brokenplugin", value="aqua.brokenplugin:get_install_dirs", group="aqua.plugins"),
]


@pytest.fixture
def mock_plugin_entrypoint(monkeypatch):
    """Register the fake aqua.plugins entry points for the duration of a single test"""

    aqua.__path__.append(os.path.join(FIXTURE_ROOT, "aqua"))

    def fake_entry_points(*args, **kwargs):
        if kwargs.get("group") == "aqua.plugins":
            return list(real_entry_points(*args, **kwargs)) + MOCK_ENTRY_POINTS
        return real_entry_points(*args, **kwargs)

    monkeypatch.setattr(components_module, "entry_points", fake_entry_points)
    discover_aqua_components.cache_clear()

    yield

    aqua.__path__.remove(os.path.join(FIXTURE_ROOT, "aqua"))
    # avoid leaking the fake plugin into tests running later in the same worker process
    discover_aqua_components.cache_clear()


@pytest.mark.aqua
class TestPluginInstall:
    """Tests exercising AquaConsole.install()/update() with a fake registered plugin"""

    def test_bare_install_includes_plugin(
        self,
        mock_plugin_entrypoint,
        tmpdir,
        set_home,
        run_aqua,
        run_aqua_console_with_input,
    ):
        """A bare `aqua install` with no component flags must also install the discovered plugin"""
        mydir = str(tmpdir)
        set_home(mydir)

        run_aqua(["install", MACHINE])

        assert os.path.isfile(os.path.join(mydir, ".aqua", "mock_config", "dummy.yaml"))
        assert os.path.isfile(os.path.join(mydir, ".aqua", "templates", "mock_templates", "dummy.tmpl"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_editable_plugin_install_creates_symlinks(
        self,
        mock_plugin_entrypoint,
        tmpdir,
        set_home,
        run_aqua,
        run_aqua_console_with_input,
    ):
        """Requesting the plugin with a path installs it in editable (symlinked) mode"""
        mydir = str(tmpdir)
        set_home(mydir)

        run_aqua(["install", MACHINE, "--core", "--mockplugin", FIXTURE_ROOT])

        assert os.path.islink(os.path.join(mydir, ".aqua", "mock_config"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_broken_plugin_does_not_crash_bare_install(
        self,
        mock_plugin_entrypoint,
        tmpdir,
        set_home,
        run_aqua,
        run_aqua_console_with_input,
    ):
        """A registered entry point whose module cannot be imported must not break a bare install"""
        mydir = str(tmpdir)
        set_home(mydir)

        run_aqua(["install", MACHINE])
        assert os.path.isfile(os.path.join(mydir, ".aqua", "config-aqua.yaml"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_discover_reports_broken_plugin_as_not_installed(self, mock_plugin_entrypoint):
        """discover_aqua_components must mark an unresolvable plugin as not installed, without raising"""
        components = discover_aqua_components()

        assert components["mockplugin"]["installed"] is True
        assert components["brokenplugin"]["installed"] is False

    def test_update_installation_updates_plugin_files(
        self,
        mock_plugin_entrypoint,
        tmpdir,
        set_home,
        run_aqua,
        run_aqua_console_with_input,
    ):
        """`aqua update` (standard mode) refreshes the plugin's installed config directory"""
        mydir = str(tmpdir)
        set_home(mydir)

        run_aqua(["install", MACHINE])
        run_aqua(["-v", "update"])

        assert os.path.isfile(os.path.join(mydir, ".aqua", "mock_config", "dummy.yaml"))

        run_aqua_console_with_input(["uninstall"], "yes")

"""Unit tests for AQUA console plugin component discovery."""

import os
from importlib.metadata import EntryPoint
from importlib.metadata import entry_points as real_entry_points

import pytest

import aqua
from aqua.core.console import components as components_module
from aqua.core.console.components import _resolve_component_info, discover_aqua_components

MOCKPLUGIN_FIXTURE_ROOT = os.path.join(os.path.dirname(__file__), "..", "fixtures", "mockplugin")
MACHINE = "github"
MOCK_ENTRY_POINTS = [
    EntryPoint(name="mockplugin", value="aqua.mockplugin:get_install_dirs", group="aqua.plugins"),
    EntryPoint(name="brokenplugin", value="aqua.brokenplugin:get_install_dirs", group="aqua.plugins"),
]


@pytest.fixture
def mock_plugin_entrypoint(monkeypatch):
    """Register fake aqua.plugins entry points for a test."""
    plugin_path = os.path.join(MOCKPLUGIN_FIXTURE_ROOT, "aqua")
    aqua.__path__.append(plugin_path)

    def fake_entry_points(*args, **kwargs):
        if kwargs.get("group") == "aqua.plugins":
            return list(real_entry_points(*args, **kwargs)) + MOCK_ENTRY_POINTS
        return real_entry_points(*args, **kwargs)

    monkeypatch.setattr(components_module, "entry_points", fake_entry_points)
    discover_aqua_components.cache_clear()
    try:
        yield
    finally:
        aqua.__path__.remove(plugin_path)
        discover_aqua_components.cache_clear()


@pytest.mark.aqua
class TestComponentResolution:
    """Tests for plugin discovery helpers in components.py."""

    def test_unknown_module_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz")
        assert info == {"installed": False, "config_dirs": [], "template_dirs": [], "path": None}

    def test_module_without_get_install_dirs_returns_not_installed(self, monkeypatch):
        monkeypatch.setattr(components_module, "import_module", lambda name: object())
        info = _resolve_component_info("somemodule")
        assert info["installed"] is False

    def test_empty_dirs_are_not_installed_without_importing(self):
        info = _resolve_component_info("whatever", data={"config": [], "templates": ["mock_templates"]})
        assert info == {"installed": False, "config_dirs": [], "template_dirs": ["mock_templates"], "path": None}

    def test_unresolvable_path_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz", data={"config": ["a"], "templates": ["b"]})
        assert info["installed"] is False

    def test_resolves_via_get_install_dirs_when_no_data_given(self, mock_plugin_entrypoint):
        info = _resolve_component_info("mockplugin")
        assert info["installed"] is True
        assert info["config_dirs"] == ["mock_config"]

    def test_discover_ignores_a_plugin_named_core(self, mock_plugin_entrypoint, monkeypatch):
        entry_points_with_core = MOCK_ENTRY_POINTS + [
            EntryPoint(name="core", value="aqua.mockplugin:get_install_dirs", group="aqua.plugins")
        ]
        monkeypatch.setattr(
            components_module,
            "entry_points",
            lambda *a, **k: entry_points_with_core if k.get("group") == "aqua.plugins" else real_entry_points(*a, **k),
        )
        discover_aqua_components.cache_clear()

        with pytest.warns(UserWarning, match="Ignoring unexpected 'core'"):
            components = discover_aqua_components()

        assert components["core"]["config_dirs"] != ["mock_config"]


@pytest.mark.aqua
@pytest.mark.console
class TestPluginInstall:
    """Tests exercising AquaConsole.install()/update() with a fake registered plugin"""

    def test_bare_install_includes_plugin(
        self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua, run_aqua_console_with_input
    ):
        """A bare `aqua install` with no component flags must also install the discovered plugin"""
        mydir = str(tmp_path)
        set_home(mydir)

        run_aqua(["install", MACHINE])

        assert os.path.isfile(os.path.join(mydir, ".aqua", "mock_config", "dummy.yaml"))
        assert os.path.isfile(os.path.join(mydir, ".aqua", "templates", "mock_templates", "dummy.tmpl"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_editable_plugin_install_creates_symlinks(
        self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua, run_aqua_console_with_input
    ):
        """Requesting the plugin with a path installs it in editable (symlinked) mode"""
        mydir = str(tmp_path)
        set_home(mydir)

        run_aqua(["install", MACHINE, "--core", "--mockplugin", MOCKPLUGIN_FIXTURE_ROOT])

        assert os.path.islink(os.path.join(mydir, ".aqua", "mock_config"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_plugin_without_core_fails_when_core_not_installed(self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua):
        """Requesting only a plugin, with core never installed here, must fail explicitly rather than
        silently installing core too"""
        mydir = str(tmp_path)
        set_home(mydir)

        with pytest.raises(SystemExit):
            run_aqua(["install", MACHINE, "--mockplugin"])

        assert not os.path.exists(os.path.join(mydir, ".aqua", "config-aqua.yaml"))

    def test_broken_plugin_does_not_crash_bare_install(
        self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua, run_aqua_console_with_input
    ):
        """A registered entry point whose module cannot be imported must not break a bare install"""
        mydir = str(tmp_path)
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
        self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua, run_aqua_console_with_input
    ):
        """`aqua update` (standard mode) refreshes the plugin's installed config directory"""
        mydir = str(tmp_path)
        set_home(mydir)

        run_aqua(["install", MACHINE])
        run_aqua(["-v", "update"])

        assert os.path.isfile(os.path.join(mydir, ".aqua", "mock_config", "dummy.yaml"))

        run_aqua_console_with_input(["uninstall"], "yes")

    def test_update_skips_editable_plugin_dirs(
        self, mock_plugin_entrypoint, tmp_path, set_home, run_aqua, run_aqua_console_with_input
    ):
        """`aqua update` must leave an editable plugin's config/template symlinks untouched"""
        mydir = str(tmp_path)
        set_home(mydir)

        run_aqua(["install", MACHINE, "--core", "--mockplugin", MOCKPLUGIN_FIXTURE_ROOT])
        run_aqua(["-v", "update"])

        assert os.path.islink(os.path.join(mydir, ".aqua", "mock_config"))
        assert os.path.islink(os.path.join(mydir, ".aqua", "templates", "mock_templates"))

        run_aqua_console_with_input(["uninstall"], "yes")

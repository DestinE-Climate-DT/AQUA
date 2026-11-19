"""Tests of the AQUA plugin contract v2 and of the per-project installation, with fake registered plugins."""

import logging
import os
import shutil
from importlib.metadata import EntryPoint
from importlib.metadata import entry_points as real_entry_points

import pytest

import aqua
from aqua.core.console import components as components_module
from aqua.core.console.components import _resolve_component_info, discover_aqua_components, validate_install_dirs
from aqua.core.console.parser import parse_arguments
from aqua.core.util import dump_yaml, load_yaml

pytestmark = pytest.mark.console

MOCKPLUGIN_FIXTURE_ROOT = os.path.join(os.path.dirname(__file__), "..", "fixtures", "mockplugin")
MACHINE = "github"
MOCK_ENTRY_POINTS = [
    EntryPoint(name="mockplugin", value="aqua.mockplugin:get_install_dirs", group="aqua.plugins"),
    EntryPoint(name="brokenplugin", value="aqua.brokenplugin:get_install_dirs", group="aqua.plugins"),
    # a plugin name starting with "c", as core: one-letter install flags would collide
    EntryPoint(name="climatemock", value="aqua.mockplugin:get_install_dirs", group="aqua.plugins"),
    # a second installable project, registered last: a bare install makes it the active project
    EntryPoint(name="mockother", value="aqua.mockother:get_install_dirs", group="aqua.plugins"),
]
OLD_CONTRACT = "plugin aqua-mockplugin uses an old plugin contract, please update it (e.g. aqua-diagnostics>=0.27.0)"


@pytest.fixture
def mock_plugin_entrypoint(monkeypatch, tmp_path_factory):
    """Register fake aqua.plugins entry points: `mockplugin`, a copy of it named `mockother`, and broken ones.

    `mockother` is generated under a temporary folder, as a second installable project.
    """
    plugin_path = os.path.join(MOCKPLUGIN_FIXTURE_ROOT, "aqua")
    other_path = str(tmp_path_factory.mktemp("mockother"))
    shutil.copytree(os.path.join(plugin_path, "mockplugin"), os.path.join(other_path, "mockother"))
    # no templates: two projects shipping the same flat folder would collide
    with open(os.path.join(other_path, "mockother", "__init__.py"), "w", encoding="utf-8") as f:
        f.write('__version__ = "0.1.0"\n\n\ndef get_install_dirs():\n    return {"contract": 2, "config": ["analysis"]}\n')
    aqua.__path__.extend([plugin_path, other_path])

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
        aqua.__path__.remove(other_path)
        discover_aqua_components.cache_clear()


@pytest.fixture
def home(tmp_path, set_home, run_aqua_console_with_input):
    """Temporary HOME: return the path of its `.aqua` folder, removed at the end of the test if installed."""
    set_home(str(tmp_path))
    configpath = os.path.join(str(tmp_path), ".aqua")
    yield configpath
    if os.path.exists(os.path.join(configpath, "config-aqua.yaml")):
        run_aqua_console_with_input(["uninstall"], "yes")


def _project(configpath):
    """Active project written in config-aqua.yaml."""
    return load_yaml(os.path.join(configpath, "config-aqua.yaml")).get("project")


class TestPluginContract:
    """The dictionary returned by get_install_dirs() and its resolution into component info."""

    def test_missing_keys_default_to_empty_lists(self):
        """Fails if a v2 plugin must declare every key, or a single folder name is not read as a list."""
        dirs = validate_install_dirs("claima", {"contract": 2, "config": ["analysis"], "files": "grids"})
        assert dirs == {"config": ["analysis"], "flat": [], "templates": [], "files": ["grids"]}

    @pytest.mark.parametrize(
        "data, match",
        [
            ({"config": ["mock_config"], "templates": ["mock_templates"]}, "uses an old plugin contract"),
            ({"contract": 1, "config": ["analysis"]}, "uses an old plugin contract"),
            ({"contract": 2, "config": ["analysis"], "extra": []}, "unknown key.s. extra in get_install_dirs"),
        ],
    )
    def test_invalid_install_dirs_are_rejected_naming_the_package(self, data, match):
        """Fails if a v1 dictionary or an unknown key is accepted, or the error omits the package."""
        with pytest.raises(ValueError, match=f"plugin aqua-mockplugin.*{match}"):
            validate_install_dirs("mockplugin", data)

    def test_unresolvable_path_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz", {"contract": 2, "config": ["a"]})
        assert (info["installed"], info["path"], info["error"]) == (False, None, None)

    def test_discover_resolves_v2_and_reports_broken_plugins(self, mock_plugin_entrypoint, monkeypatch):
        """Fails if core is not flat, a v2 plugin is not installable, or a broken/old plugin is not reported."""
        components = discover_aqua_components()

        assert components["core"]["config"] == []
        assert components["core"]["flat"] == ["catgen", "data_model", "fixes", "grids", "styles"]
        assert components["mockplugin"]["installed"] is True
        assert (components["mockplugin"]["config"], components["mockplugin"]["files"]) == (["analysis"], ["grids"])
        assert (components["brokenplugin"]["installed"], components["brokenplugin"]["error"]) == (False, None)

        monkeypatch.setattr("aqua.mockplugin.get_install_dirs", lambda: {"config": ["analysis"]})
        discover_aqua_components.cache_clear()
        components = discover_aqua_components()
        assert (components["mockplugin"]["installed"], components["mockplugin"]["error"]) == (False, OLD_CONTRACT)

    def test_install_flags_do_not_collide(self, mock_plugin_entrypoint):
        """Only core and diagnostics keep a one-letter install flag: with one per plugin, `climatemock` would
        register `-c` again and the parser could not be built."""
        parser = parse_arguments()["main"]

        args = parser.parse_args(["install", MACHINE, "-c", "--climatemock", "--mockplugin", "/some/path"])
        assert (args.core, args.climatemock, args.mockplugin) == ("standard", "standard", "/some/path")
        with pytest.raises(SystemExit) as excinfo:
            parser.parse_args(["install", MACHINE, "-m"])
        assert excinfo.value.code == 2


class TestPluginInstall:
    """`aqua install`, `update`, `list` and `check` with the fake plugins."""

    def test_bare_install_nests_projects_and_activates_the_last(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if project folders are installed flat, a project is missing, or the last project is not active."""
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        run_aqua(["install", MACHINE])

        for project in ("mockplugin", "mockother"):
            assert os.path.isfile(os.path.join(home, "analysis", project, "dummy.yaml"))
        assert not os.path.exists(os.path.join(home, "analysis", "dummy.yaml"))
        assert os.path.isfile(os.path.join(home, "templates", "mock_templates", "dummy.tmpl"))
        assert os.path.isdir(os.path.join(home, "catgen"))  # core folders stay flat
        assert _project(home) == "mockother"
        assert any("the last one, mockother, is now the active project" in r.getMessage() for r in caplog.records)

    def test_adding_a_project_makes_it_active(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if core alone changes the template project, or adding a project does not switch to it."""
        run_aqua(["install", MACHINE, "--core"])
        assert not os.path.exists(os.path.join(home, "analysis"))
        assert _project(home) == "diagnostics"

        run_aqua(["install", MACHINE, "--mockplugin"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert _project(home) == "mockplugin"

        run_aqua(["install", MACHINE, "--mockother"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert _project(home) == "mockother"

    def test_editable_install_links_per_project_and_update_skips_it(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if the editable link is not made at `<dir>/<project>`, or `aqua update` replaces it."""
        run_aqua(["install", MACHINE, "--core", "--mockplugin", MOCKPLUGIN_FIXTURE_ROOT])
        link = os.path.join(home, "analysis", "mockplugin")

        assert os.path.islink(link) and not os.path.islink(os.path.join(home, "analysis"))
        assert os.path.realpath(link) == os.path.realpath(
            os.path.join(MOCKPLUGIN_FIXTURE_ROOT, "aqua", "mockplugin", "config", "analysis")
        )

        run_aqua(["update"])
        assert os.path.islink(link)
        assert os.path.islink(os.path.join(home, "templates", "mock_templates"))

    def test_update_refreshes_projects_and_preserves_others(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if `aqua update` does not restore a shipped file, or deletes a folder of another project."""
        run_aqua(["install", MACHINE])
        shipped = os.path.join(home, "analysis", "mockplugin", "dummy.yaml")
        os.remove(shipped)
        foreign = os.path.join(home, "analysis", "foreign", "user.yaml")
        os.makedirs(os.path.dirname(foreign))
        dump_yaml(foreign, {"user": True})

        run_aqua(["update"])

        assert os.path.isfile(shipped)
        assert os.path.isfile(os.path.join(home, "analysis", "mockother", "dummy.yaml"))
        assert load_yaml(foreign) == {"user": True}

    def test_old_contract_plugin_stops_install(self, mock_plugin_entrypoint, monkeypatch, home, run_aqua, aqua_caplog):
        """Fails if a plugin with the v1 contract is installed, or the installation starts before the error."""
        monkeypatch.setattr("aqua.mockplugin.get_install_dirs", lambda: {"config": ["analysis"], "templates": []})
        discover_aqua_components.cache_clear()
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE])

        assert excinfo.value.code == 1
        assert any(r.getMessage() == OLD_CONTRACT for r in caplog.records)
        assert not os.path.exists(os.path.join(home, "config-aqua.yaml"))

    def test_plugin_without_core_fails_when_core_not_installed(self, mock_plugin_entrypoint, home, run_aqua):
        """Requesting only a plugin, with core never installed here, must fail explicitly rather than
        silently installing core too"""
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE, "--mockplugin"])

        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(home, "config-aqua.yaml"))

    def test_invalid_editable_plugin_path_fails(self, mock_plugin_entrypoint, tmp_path, home, run_aqua):
        """An invalid editable plugin path must fail after a core-only installation."""
        run_aqua(["install", MACHINE, "--core"])

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE, "--mockplugin", str(tmp_path / "missing-plugin")])

        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(home, "analysis", "mockplugin"))

    def test_list_shows_projects_with_mode_version_and_active(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua list` misses a project, its mode, its package version or the active mark."""
        run_aqua(["install", MACHINE, "--core", "--mockplugin", MOCKPLUGIN_FIXTURE_ROOT])
        run_aqua(["install", MACHINE, "--mockother"])
        capfd.readouterr()

        run_aqua(["list"])

        out = capfd.readouterr().out.splitlines()
        assert "* mockother (standard, aqua-mockother 0.1.0)" in out
        assert "  mockplugin (editable, aqua-mockplugin 0.1.0)" in out

    def test_list_all_skips_plugins_not_installed(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua list -a` lists (or crashes on) the folders of a plugin available but not installed."""
        run_aqua(["install", MACHINE, "--core"])
        capfd.readouterr()

        run_aqua(["list", "-a"])

        out = capfd.readouterr().out
        assert f"AQUA current installed grids in {home}:" in out
        assert "analysis" not in out and "mock_templates" not in out

    def test_check_valid_installation(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua check` rejects an installation with its active project and a loadable catalog."""
        run_aqua(["install", MACHINE])
        os.makedirs(os.path.join(home, "catalogs", "good"))
        dump_yaml(os.path.join(home, "catalogs", "good", "catalog.yaml"), {"sources": {}})
        capfd.readouterr()

        run_aqua(["check"])

        assert f"AQUA installation in {home} is valid (1 catalogs checked)" in capfd.readouterr().out

    @pytest.mark.parametrize(
        "breakage, message",
        [
            ("legacy", "This installation predates project support (no project key in"),
            ("project", "The active project ghost is not installed (installed: "),
            ("catalog", "Catalog bad cannot be loaded from"),
        ],
    )
    def test_check_failures(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog, breakage, message):
        """Fails if `aqua check` exits with 0 on a pre-project installation, a missing active project or a bad catalog."""
        run_aqua(["install", MACHINE])
        configfile = os.path.join(home, "config-aqua.yaml")
        config = load_yaml(configfile)
        if breakage == "legacy":
            del config["project"]
        elif breakage == "project":
            config["project"] = "ghost"
        else:
            os.makedirs(os.path.join(home, "catalogs", "bad"))
            with open(os.path.join(home, "catalogs", "bad", "catalog.yaml"), "w", encoding="utf-8") as f:
                f.write("sources: [unclosed\n")
        dump_yaml(configfile, config)
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["check"])

        assert excinfo.value.code == 1
        assert any(r.getMessage().startswith(message) for r in caplog.records)

    def test_update_refuses_pre_project_installation(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if `aqua update` writes nested project folders into an installation with the flat layout."""
        run_aqua(["install", MACHINE, "--core"])
        configfile = os.path.join(home, "config-aqua.yaml")
        config = load_yaml(configfile)
        del config["project"]
        dump_yaml(configfile, config)

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["update"])

        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(home, "analysis"))

    def test_broken_plugin_does_not_crash_bare_install(self, mock_plugin_entrypoint, home, run_aqua):
        """A registered entry point whose module cannot be imported must not break a bare install"""
        run_aqua(["install", MACHINE])
        assert os.path.isfile(os.path.join(home, "config-aqua.yaml"))

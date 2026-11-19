"""Tests of the AQUA plugin contract v2 and of the per-project installation, with fake registered plugins."""

import logging
import os
import shutil
from importlib.metadata import EntryPoint
from importlib.metadata import entry_points as real_entry_points

import pytest

import aqua
from aqua.core.console import components as components_module
from aqua.core.console.components import (
    PluginContractError,
    _resolve_component_info,
    discover_aqua_components,
    validate_install_dirs,
)
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


def _install_projects(run_aqua, *projects):
    """Install core, then the given projects with `aqua project install`."""
    run_aqua(["install", MACHINE, "--core"])
    for project in projects:
        run_aqua(["project", "install", project])


def _project(configpath):
    """Active project written in config-aqua.yaml."""
    return load_yaml(os.path.join(configpath, "config-aqua.yaml")).get("project")


class TestPluginContract:
    """The dictionary returned by get_install_dirs() and its resolution into component info."""

    def test_missing_keys_default_to_empty_lists(self):
        """Fails if a v2 plugin must declare every key, or the lists end up under the wrong info key."""
        dirs = validate_install_dirs("claima", {"contract": 2, "config": ["analysis"], "files": ["grids"]})
        assert dirs == {"config_dirs": ["analysis"], "flat_dirs": [], "template_dirs": [], "file_dirs": ["grids"]}

    @pytest.mark.parametrize(
        "data, match",
        [
            ({"config": ["mock_config"], "templates": ["mock_templates"]}, "uses an old plugin contract"),
            ({"contract": 1, "config": ["analysis"]}, "uses an old plugin contract"),
            ({"contract": 2, "config": ["analysis"], "extra": []}, "unknown key.s. extra in get_install_dirs"),
            ({"contract": 2, "config": "analysis"}, "'config' in get_install_dirs.. must be a list"),
        ],
    )
    def test_invalid_install_dirs_are_rejected_naming_the_package(self, data, match):
        """Fails if a v1 dictionary, an unknown key or a non-list value is accepted, or the error omits the package."""
        with pytest.raises(PluginContractError, match=f"plugin aqua-mockplugin.*{match}"):
            validate_install_dirs("mockplugin", data)

    def test_unknown_module_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz")
        assert (info["installed"], info["path"], info["error"]) == (False, None, None)

    def test_module_without_get_install_dirs_returns_not_installed(self, monkeypatch):
        monkeypatch.setattr(components_module, "import_module", lambda name: object())
        assert _resolve_component_info("somemodule")["installed"] is False

    def test_unresolvable_path_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz", data={"contract": 2, "config": ["a"]})
        assert info["installed"] is False

    def test_core_is_flat_and_not_a_project(self):
        """Fails if core installs a project-scoped folder (catgen included) or is reported as a project."""
        info = _resolve_component_info("core")
        assert (info["project"], info["config_dirs"]) == (False, [])
        assert info["flat_dirs"] == ["catgen", "data_model", "fixes", "grids", "styles"]

    def test_discover_resolves_v2_and_reports_broken_plugins(self, mock_plugin_entrypoint, monkeypatch):
        """Fails if a v2 plugin is not installable, or a broken/old plugin raises instead of being reported."""
        components = discover_aqua_components()

        assert components["mockplugin"]["installed"] is True
        assert components["mockplugin"]["config_dirs"] == ["analysis"]
        assert components["mockplugin"]["file_dirs"] == ["grids"]
        assert (components["brokenplugin"]["installed"], components["brokenplugin"]["error"]) == (False, None)

        monkeypatch.setattr("aqua.mockplugin.get_install_dirs", lambda: {"config": ["analysis"]})
        discover_aqua_components.cache_clear()
        components = discover_aqua_components()
        assert (components["mockplugin"]["installed"], components["mockplugin"]["error"]) == (False, OLD_CONTRACT)

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

        assert components["core"]["config_dirs"] == []

    def test_install_has_only_core_and_diagnostics_flags(self, mock_plugin_entrypoint):
        """Fails if a discovered plugin gets its own install flag, or -c/-d do not take an optional path."""
        parser = parse_arguments()["main"]

        args = parser.parse_args(["install", MACHINE, "-c", "-d", "/some/path"])
        assert (args.core, args.diagnostics) == ("standard", "/some/path")
        with pytest.raises(SystemExit) as excinfo:
            parser.parse_args(["install", MACHINE, "--mockplugin"])
        assert excinfo.value.code == 2


class TestPluginInstall:
    """`aqua install`, `update`, `list` and `check` with the fake plugins."""

    def test_bare_install_skips_other_projects(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if a bare install installs a discovered project other than diagnostics, or changes the active one."""
        run_aqua(["install", MACHINE])

        assert os.path.isdir(os.path.join(home, "catgen"))  # core folders stay flat
        assert not os.path.exists(os.path.join(home, "analysis", "mockplugin"))
        assert _project(home) == "diagnostics"

    def test_adding_a_project_makes_it_active(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if core alone changes the template project, project folders are installed flat, or adding a
        project does not switch to it."""
        run_aqua(["install", MACHINE, "--core"])
        assert not os.path.exists(os.path.join(home, "analysis"))
        assert _project(home) == "diagnostics"

        run_aqua(["project", "install", "mockplugin"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert not os.path.exists(os.path.join(home, "analysis", "dummy.yaml"))
        assert os.path.isfile(os.path.join(home, "templates", "mock_templates", "dummy.tmpl"))
        assert _project(home) == "mockplugin"

        run_aqua(["project", "install", "mockother"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert _project(home) == "mockother"

    def test_editable_install_links_per_project_and_update_skips_it(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if the editable link is not made at `<dir>/<project>`, or `aqua update` replaces it."""
        run_aqua(["install", MACHINE, "--core"])
        run_aqua(["project", "install", "mockplugin", "-e", MOCKPLUGIN_FIXTURE_ROOT])
        link = os.path.join(home, "analysis", "mockplugin")

        assert os.path.islink(link) and not os.path.islink(os.path.join(home, "analysis"))
        assert os.path.realpath(link) == os.path.realpath(
            os.path.join(MOCKPLUGIN_FIXTURE_ROOT, "aqua", "mockplugin", "config", "analysis")
        )

        run_aqua(["update"])
        assert os.path.islink(link)
        assert os.path.islink(os.path.join(home, "templates", "mock_templates"))

    def test_update_refreshes_projects_and_preserves_others(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if `aqua update` does not restore a shipped file, deletes a user file or a folder of another project,
        or loses the extra files of a project when core refreshes its flat folders."""
        _install_projects(run_aqua, "mockplugin", "mockother")
        shipped = os.path.join(home, "analysis", "mockplugin", "dummy.yaml")
        os.remove(shipped)
        foreign = os.path.join(home, "analysis", "foreign", "user.yaml")
        os.makedirs(os.path.dirname(foreign))
        dump_yaml(foreign, {"user": True})

        user = os.path.join(home, "analysis", "mockplugin", "user.yaml")
        dump_yaml(user, {"user": True})

        run_aqua(["update"])

        assert os.path.isfile(shipped)
        assert os.path.isfile(os.path.join(home, "analysis", "mockother", "dummy.yaml"))
        assert load_yaml(foreign) == {"user": True}
        assert load_yaml(user) == {"user": True}
        # core replaces its flat grids folder: the extra file of the project must be added again
        assert os.path.isfile(os.path.join(home, "grids", "mockplugin-grid.yaml"))

    def test_old_contract_plugin_stops_install(self, mock_plugin_entrypoint, monkeypatch, home, run_aqua, aqua_caplog):
        """Fails if a plugin with the v1 contract is installed, or the installation starts before the error."""
        run_aqua(["install", MACHINE, "--core"])
        monkeypatch.setattr("aqua.mockplugin.get_install_dirs", lambda: {"config": ["analysis"], "templates": []})
        discover_aqua_components.cache_clear()
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["project", "install", "mockplugin"])

        assert excinfo.value.code == 1
        assert any(r.getMessage() == OLD_CONTRACT for r in caplog.records)
        assert not os.path.exists(os.path.join(home, "analysis", "mockplugin"))

    def test_diagnostics_without_core_fails_when_core_not_installed(self, mock_plugin_entrypoint, home, run_aqua):
        """Requesting only diagnostics, with core never installed here, must fail explicitly rather than
        silently installing core too"""
        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE, "-d"])

        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(home, "config-aqua.yaml"))

    def test_invalid_editable_plugin_path_fails(self, mock_plugin_entrypoint, tmp_path, home, run_aqua):
        """An invalid editable plugin path must fail after a core-only installation."""
        run_aqua(["install", MACHINE, "--core"])

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["project", "install", "mockplugin", "-e", str(tmp_path / "missing-plugin")])

        assert excinfo.value.code == 1
        assert not os.path.exists(os.path.join(home, "analysis", "mockplugin"))

    def test_list_shows_projects_with_mode_version_and_active(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua list` misses a project, its mode, its package version or the active mark."""
        run_aqua(["install", MACHINE, "--core"])
        run_aqua(["project", "install", "mockplugin", "-e", MOCKPLUGIN_FIXTURE_ROOT])
        run_aqua(["project", "install", "mockother"])
        capfd.readouterr()

        run_aqua(["list"])

        out = capfd.readouterr().out.splitlines()
        assert "* mockother (standard, aqua-mockother 0.1.0)" in out
        assert "  mockplugin (editable, aqua-mockplugin 0.1.0)" in out

    def test_check_valid_installation(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua check` rejects an installation with its active project and a loadable catalog."""
        _install_projects(run_aqua, "mockplugin")
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
        _install_projects(run_aqua, "mockplugin")
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


MOCKPLUGIN_CONFIG = os.path.join(MOCKPLUGIN_FIXTURE_ROOT, "aqua", "mockplugin", "config")


def _mockplugin_paths(home):
    """Installed folders and extra file of `mockplugin`."""
    return {
        "analysis": os.path.join(home, "analysis", "mockplugin"),
        "templates": os.path.join(home, "templates", "mock_templates"),
        "grid": os.path.join(home, "grids", "mockplugin-grid.yaml"),
    }


class TestProjectVerbs:
    """`aqua project install`, `update` and `remove` with the fake plugins, on a core installation."""

    def test_install_update_remove_standard(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if a project or its extra file is not copied, update does not restore shipped files or deletes a user
        file, or remove leaves a folder or the extra file behind or does not reset the active project."""
        run_aqua(["install", MACHINE, "--core"])
        paths = _mockplugin_paths(home)

        run_aqua(["project", "install", "mockplugin"])
        assert all(os.path.exists(p) and not os.path.islink(p) for p in paths.values())
        assert _project(home) == "mockplugin"

        shipped = os.path.join(paths["analysis"], "dummy.yaml")
        user = os.path.join(paths["analysis"], "user.yaml")
        dump_yaml(shipped, {"dummy": False})
        dump_yaml(paths["grid"], {"grids": {}})
        dump_yaml(user, {"user": True})
        run_aqua(["project", "update", "mockplugin"])
        assert load_yaml(shipped) == {"dummy": True}
        assert load_yaml(paths["grid"]) == load_yaml(os.path.join(MOCKPLUGIN_CONFIG, "grids", "mockplugin-grid.yaml"))
        assert load_yaml(user) == {"user": True}

        run_aqua(["project", "remove", "mockplugin"])
        assert not any(os.path.lexists(p) for p in paths.values())
        assert os.path.isdir(os.path.join(home, "grids"))  # core folder kept
        assert _project(home) == "diagnostics"

    def test_install_update_remove_editable(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if an editable project or its extra file is not linked, update replaces a link, or remove deletes
        the linked sources instead of the links."""
        _install_projects(run_aqua, "mockother")
        paths = _mockplugin_paths(home)

        run_aqua(["project", "install", "mockplugin", "-e", MOCKPLUGIN_FIXTURE_ROOT])
        assert all(os.path.islink(p) for p in paths.values())
        assert os.path.realpath(paths["grid"]) == os.path.realpath(
            os.path.join(MOCKPLUGIN_CONFIG, "grids", "mockplugin-grid.yaml")
        )

        run_aqua(["project", "update", "mockplugin"])
        assert all(os.path.islink(p) for p in paths.values())

        run_aqua(["project", "set", "mockother"])
        run_aqua(["project", "remove", "mockplugin"])
        assert not any(os.path.lexists(p) for p in paths.values())
        assert os.path.isfile(os.path.join(MOCKPLUGIN_CONFIG, "grids", "mockplugin-grid.yaml"))
        assert _project(home) == "mockother"  # an inactive project is removed without changing the active one

    @pytest.mark.parametrize(
        "command, message",
        [
            (["project", "install", "mockplugin"], "Project mockplugin is already installed"),
            (["project", "install", "ghost"], "aqua.ghost package not found"),
            (["project", "update", "mockother"], "Project mockother is not installed"),
            (["project", "remove", "mockother"], "Project mockother is not installed"),
            (["install", MACHINE, "-d"], "aqua.diagnostics package not found"),
        ],
    )
    def test_project_verb_errors(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog, command, message):
        """Fails if a project is installed twice, a missing package is installed, or a project not installed is
        updated or removed."""
        run_aqua(["install", MACHINE, "--core"])
        run_aqua(["project", "install", "mockplugin"])
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(command)

        assert excinfo.value.code == 1
        assert any(r.getMessage().startswith(message) for r in caplog.records)
        assert _project(home) == "mockplugin"

    def test_extra_file_collision_stops_install(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if an extra file overwrites a file with the same name, or the project is installed anyway."""
        run_aqua(["install", MACHINE, "--core"])
        paths = _mockplugin_paths(home)
        dump_yaml(paths["grid"], {"grids": {}})
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["project", "install", "mockplugin"])

        assert excinfo.value.code == 1
        assert any(
            r.getMessage() == f"Cannot install mockplugin, these files already exist: {paths['grid']}" for r in caplog.records
        )
        assert load_yaml(paths["grid"]) == {"grids": {}}
        assert not os.path.exists(paths["analysis"])

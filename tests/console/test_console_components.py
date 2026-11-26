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
    # a second installable project
    EntryPoint(name="mockother", value="aqua.mockother:get_install_dirs", group="aqua.plugins"),
]
OLD_CONTRACT = "plugin aqua-mockplugin uses an old plugin contract, please update it (e.g. aqua-diagnostics>=0.27.0)"


@pytest.fixture
def mock_plugin_entrypoint(monkeypatch, tmp_path_factory):
    """Register only fake aqua.plugins entry points: `mockplugin`, a copy of it named `mockother`, and broken ones.

    `mockother` is generated under a temporary folder, as a second installable project.
    `mockplugin` stands for aqua-diagnostics as the project allowed to install templates folders.
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
            return list(MOCK_ENTRY_POINTS)  # never the real plugins: the tests must not depend on the environment
        return real_entry_points(*args, **kwargs)

    monkeypatch.setattr(components_module, "entry_points", fake_entry_points)
    monkeypatch.setattr(components_module, "TEMPLATES_PROJECTS", ("mockplugin",))
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
    """Project key of config-aqua.yaml: the installed projects, the active one first (a single name is a string)."""
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
            ({"contract": 2, "flat": ["definitions", "grids", "tools"]}, "flat folder.s. grids, tools reserved by aqua-core"),
            ({"contract": 2, "templates": ["mock_templates"]}, "only aqua-diagnostics can install templates folders"),
            ({"contract": 2, "config": ["tools", "grids"]}, "config folder.s. grids not allowed, choose among analysis"),
            ({"contract": 2, "files": ["styles", "catalogs"]}, "files folder.s. catalogs not allowed, choose among grids"),
        ],
    )
    def test_invalid_install_dirs_are_rejected_naming_the_package(self, data, match):
        """Fails if a v1 dictionary, an unknown key, a flat folder of core or of the project layout, a templates
        folder of a project other than diagnostics, a config folder that is not project-scoped or a files folder other
        than grids, fixes and styles is accepted, or the error omits the package."""
        with pytest.raises(ValueError, match=f"plugin aqua-mockplugin.*{match}"):
            validate_install_dirs("mockplugin", data)

    def test_only_diagnostics_installs_templates(self):
        """Fails if aqua-diagnostics cannot declare a templates folder, or can declare one of core."""
        dirs = validate_install_dirs("diagnostics", {"contract": 2, "templates": "collections"})
        assert dirs["templates"] == ["collections"]
        with pytest.raises(ValueError, match="plugin aqua-diagnostics: templates folder.s. drop reserved by aqua-core"):
            validate_install_dirs("diagnostics", {"contract": 2, "templates": ["collections", "drop"]})

    def test_unresolvable_path_returns_not_installed(self):
        info = _resolve_component_info("doesnotexist_xyz", {"contract": 2, "config": ["analysis"]})
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
        """Fails if a bare install installs a discovered project other than diagnostics, makes it active, leaves
        `diagnostics` active without installing it, or drops the project key (the empty key marks an installation
        with project support)."""
        run_aqua(["install", MACHINE])

        assert os.path.isdir(os.path.join(home, "catgen"))  # core folders stay flat
        assert not os.path.exists(os.path.join(home, "analysis", "mockplugin"))
        # the fake plugins have no diagnostics package
        assert load_yaml(os.path.join(home, "config-aqua.yaml")).get("project", "missing") is None

    def test_adding_a_project_makes_it_active(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if core alone changes the template project, project folders are installed flat, or adding a
        project does not switch to it."""
        run_aqua(["install", MACHINE, "--core"])
        assert not os.path.exists(os.path.join(home, "analysis"))
        assert load_yaml(os.path.join(home, "config-aqua.yaml")).get("project", "missing") is None

        run_aqua(["project", "install", "mockplugin"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert not os.path.exists(os.path.join(home, "analysis", "dummy.yaml"))
        assert os.path.isfile(os.path.join(home, "templates", "mock_templates", "dummy.tmpl"))
        assert _project(home) == "mockplugin"

        run_aqua(["project", "install", "mockother"])
        assert os.path.isfile(os.path.join(home, "analysis", "mockplugin", "dummy.yaml"))
        assert _project(home) == ["mockother", "mockplugin"]

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
        or loses the extra files of a project when core replaces its flat folders."""
        _install_projects(run_aqua, "mockplugin", "mockother")
        shipped = os.path.join(home, "analysis", "mockplugin", "dummy.yaml")
        os.remove(shipped)
        user = os.path.join(home, "analysis", "mockplugin", "user.yaml")
        dump_yaml(user, {"user": True})
        foreign = os.path.join(home, "analysis", "foreign", "user.yaml")
        os.makedirs(os.path.dirname(foreign))
        dump_yaml(foreign, {"user": True})

        run_aqua(["update"])

        assert os.path.isfile(shipped)
        assert os.path.isfile(os.path.join(home, "analysis", "mockother", "dummy.yaml"))
        assert load_yaml(foreign) == {"user": True}
        assert load_yaml(user) == {"user": True}
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

    def test_list_shows_projects_with_mode_and_version(self, mock_plugin_entrypoint, home, run_aqua, capfd):
        """Fails if `aqua list` misses a project, its mode or its package version."""
        run_aqua(["install", MACHINE, "--core"])
        run_aqua(["project", "install", "mockplugin", "-e", MOCKPLUGIN_FIXTURE_ROOT])
        run_aqua(["project", "install", "mockother"])
        capfd.readouterr()

        run_aqua(["list"])

        out = capfd.readouterr().out.splitlines()
        assert "\t - mockother (standard, aqua-mockother 0.1.0)" in out
        assert "\t - mockplugin (editable, aqua-mockplugin 0.1.0)" in out

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
        _install_projects(run_aqua, "mockplugin")
        os.makedirs(os.path.join(home, "catalogs", "good"))
        dump_yaml(os.path.join(home, "catalogs", "good", "catalog.yaml"), {"sources": {}})
        capfd.readouterr()

        run_aqua(["check"])

        assert f"AQUA installation in {home} is valid (1 catalogs checked)" in capfd.readouterr().out

    @pytest.mark.parametrize(
        "breakage, message",
        [
            ("legacy", "No project set in"),
            ("project", "Project ghost is listed in"),
            ("project", "analysis/mockplugin does not belong to an installed project"),
            ("catalog", "Catalog bad cannot be loaded from"),
        ],
    )
    def test_check_failures(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog, breakage, message):
        """Fails if `aqua check` exits with 0 on a pre-project installation, a listed project without folders or a bad
        catalog, or does not report a project folder that is not listed."""
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
    """Project folder, flat folder, templates folder and extra file installed by `mockplugin`."""
    return {
        "analysis": os.path.join(home, "analysis", "mockplugin"),
        "flat": os.path.join(home, "mock_flat"),
        "templates": os.path.join(home, "templates", "mock_templates"),
        "grid": os.path.join(home, "grids", "mockplugin-grid.yaml"),
    }


class TestProjectVerbs:
    """`aqua project install`, `update` and `remove` with the fake plugins, on a core installation."""

    def test_install_update_remove_standard(self, mock_plugin_entrypoint, home, run_aqua):
        """Fails if a project or its extra file is not copied, update does not restore shipped files or deletes a user
        file, or remove leaves the project folder, the flat folder, the templates folder or the extra file behind, or
        deletes a folder of core or of another project, or if `aqua update` installs again the removed project."""
        _install_projects(run_aqua, "mockother")
        paths = _mockplugin_paths(home)

        run_aqua(["project", "install", "mockplugin"])
        assert all(os.path.exists(p) and not os.path.islink(p) for p in paths.values())
        assert _project(home) == ["mockplugin", "mockother"]

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
        # the folders of core and of the other project stay
        assert os.path.isdir(os.path.join(home, "grids"))
        assert os.path.isdir(os.path.join(home, "templates", "drop"))
        assert os.path.isdir(os.path.join(home, "analysis", "mockother"))

        # the package is still there, but the project is not installed any more
        run_aqua(["update"])
        assert not any(os.path.lexists(p) for p in paths.values())

    def test_install_update_remove_editable(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if an editable project or its extra file is not linked, `project update` replaces a link or does not
        say that there is nothing to update, `aqua update` loses the link of the extra file when core replaces its
        grids folder (or keeps there a file that is not a link), or remove deletes the linked sources instead of the
        links."""
        run_aqua(["install", MACHINE, "--core"])
        paths = _mockplugin_paths(home)

        run_aqua(["project", "install", "mockplugin", "-e", MOCKPLUGIN_FIXTURE_ROOT])
        assert all(os.path.islink(p) for p in paths.values())
        assert os.path.realpath(paths["grid"]) == os.path.realpath(
            os.path.join(MOCKPLUGIN_CONFIG, "grids", "mockplugin-grid.yaml")
        )

        caplog = aqua_caplog(logging.getLogger("AQUA"))
        run_aqua(["project", "update", "mockplugin"])
        assert all(os.path.islink(p) for p in paths.values())
        assert [r.getMessage() for r in caplog.records if "editable mode" in r.getMessage()] == [
            "mockplugin is installed in editable mode: it follows its repository, nothing to update"
        ]

        user = os.path.join(home, "grids", "user-grid.yaml")
        dump_yaml(user, {"grids": {}})
        run_aqua(["update"])
        assert all(os.path.islink(p) for p in paths.values())
        assert not os.path.exists(user) and os.path.isfile(os.path.join(home, "grids", "bcc.yaml"))

        run_aqua(["project", "remove", "mockplugin"])
        assert not any(os.path.lexists(p) for p in paths.values())
        assert os.path.isfile(os.path.join(MOCKPLUGIN_CONFIG, "grids", "mockplugin-grid.yaml"))

    def test_remove_active_project_switches_to_another(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if removing the active project does not activate another installed one, or if removing the last
        project leaves its name active or makes the installation look older than project support."""
        _install_projects(run_aqua, "mockother", "mockplugin")
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        run_aqua(["project", "remove", "mockplugin"])
        assert _project(home) == "mockother"

        run_aqua(["project", "remove", "mockother"])
        config = load_yaml(os.path.join(home, "config-aqua.yaml"))
        assert "project" in config and config["project"] is None
        assert [r.getMessage() for r in caplog.records if r.getMessage().startswith("Removed the active project")] == [
            "Removed the active project mockplugin, mockother is now the active project",
            "Removed the active project mockother, no project is installed",
        ]
        run_aqua(["project", "install", "mockplugin"])  # the installation still has the project layout
        assert _project(home) == "mockplugin"

    @pytest.mark.parametrize(
        "command, message",
        [
            (["project", "install", "mockplugin"], "Project mockplugin is already installed"),
            (["project", "install", "ghost"], "aqua.ghost package not found"),
            (["project", "update", "mockother"], "Project mockother is not installed"),
            (["project", "remove", "mockother"], "Project mockother is not installed"),
        ],
    )
    def test_project_verb_errors(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog, command, message):
        """Fails if a project is installed twice, a missing package is installed, or a project not installed is
        updated or removed."""
        _install_projects(run_aqua, "mockplugin")
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(command)

        assert excinfo.value.code == 1
        assert any(r.getMessage().startswith(message) for r in caplog.records)
        assert _project(home) == "mockplugin"

    def test_diagnostics_only_install_fails_without_the_package(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if `aqua install -d` succeeds, or changes the active project, when aqua-diagnostics is missing."""
        _install_projects(run_aqua, "mockplugin")
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["install", MACHINE, "-d"])

        assert excinfo.value.code == 1
        assert any(r.getMessage().startswith("aqua.diagnostics package not found") for r in caplog.records)
        assert _project(home) == "mockplugin"

    @pytest.mark.parametrize("clash, existing", [("grid", "grid"), ("flat", "flat_file")])
    def test_existing_flat_folder_or_extra_file_stops_install(
        self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog, clash, existing
    ):
        """Fails if an extra file or a flat folder of a project overwrites one already there (that `project remove`
        would then delete), or the project is installed anyway."""
        run_aqua(["install", MACHINE, "--core"])
        paths = _mockplugin_paths(home)
        paths["flat_file"] = os.path.join(paths["flat"], "user.yaml")
        os.makedirs(os.path.dirname(paths[existing]), exist_ok=True)
        dump_yaml(paths[existing], {"user": True})
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        with pytest.raises(SystemExit) as excinfo:
            run_aqua(["project", "install", "mockplugin"])

        assert excinfo.value.code == 1
        expected = f"Cannot install mockplugin, these files or folders already exist: {paths[clash]}"
        assert any(r.getMessage() == expected for r in caplog.records)
        assert load_yaml(paths[existing]) == {"user": True}
        assert not os.path.exists(paths["analysis"])

    @pytest.mark.skipif(hasattr(os, "geteuid") and os.geteuid() == 0, reason="root ignores folder permissions")
    def test_failed_copy_removes_the_partial_project(self, mock_plugin_entrypoint, home, run_aqua, aqua_caplog):
        """Fails if a copy failing midway leaves the folders copied before it, so that the project looks installed."""
        run_aqua(["install", MACHINE, "--core"])
        paths = _mockplugin_paths(home)
        templates = os.path.join(home, "templates", "mock_templates")
        grids = os.path.dirname(paths["grid"])
        caplog = aqua_caplog(logging.getLogger("AQUA"))

        os.chmod(grids, 0o555)  # the extra grid file is copied last, after the folders
        try:
            with pytest.raises(SystemExit) as excinfo:
                run_aqua(["project", "install", "mockplugin"])
        finally:
            os.chmod(grids, 0o755)

        assert excinfo.value.code == 1
        assert any(r.getMessage().startswith("Cannot install mockplugin: ") for r in caplog.records)
        assert not any(os.path.lexists(p) for p in [*paths.values(), templates])
        assert _project(home) != "mockplugin"

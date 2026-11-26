import os
import shutil

import pytest

from aqua import show_catalog_content
from aqua.core.configurer import ConfigCatalog, ConfigContext
from aqua.core.util import dump_yaml


@pytest.mark.aqua
def test_config_plain():
    config = ConfigContext()
    assert "config-aqua.yaml" == os.path.basename(config.config_file)
    assert "ci" in config.config_dict["catalog"]


@pytest.mark.aqua
def test_config_paths():
    configfile = "tests/config/config-aqua-custom.yaml"

    configdir = ConfigContext().get_config_dir()

    # Copy the file to the config directory
    shutil.copy(configfile, configdir)

    config = ConfigCatalog(catalog="ci", configcontext=ConfigContext(filename="config-aqua-custom.yaml", configdir=configdir))
    paths, _ = config.get_machine_info()

    assert paths["paths"]["grids"] == "pluto"

    # Remove the copied file
    os.remove(f"{configdir}/config-aqua-custom.yaml")


@pytest.mark.aqua
def test_show_catalog_content_basic():
    """Test show_catalog_content with no filters."""
    results = show_catalog_content()

    assert isinstance(results, dict)
    # Check structure: catalog -> model -> exp -> list of sources
    for _, catalog_data in results.items():
        assert isinstance(catalog_data, dict)
        for _, model_data in catalog_data.items():
            assert isinstance(model_data, dict)
            for _, sources in model_data.items():
                assert isinstance(sources, list)


def _project_install(root, project=None, folders=("analysis/diagnostics", "analysis/claima", "tools")):
    """Write a minimal installation under ``root``, with the project key only if given (``""`` for an empty key):
    a name, or the list of the installed projects with the active one first."""
    config = {"machine": "ci"} if project is None else {"machine": "ci", "project": project or None}
    dump_yaml(os.path.join(root, "config-aqua.yaml"), config)
    for folder in folders:
        os.makedirs(os.path.join(root, folder))
    return str(root)


@pytest.mark.aqua
@pytest.mark.parametrize(
    "key, env, expected, subpath",
    [
        ("diagnostics", None, "diagnostics", "analysis/diagnostics"),
        (["claima", "diagnostics"], None, "claima", "analysis/claima"),
        (["diagnostics", "claima"], "claima", "claima", "analysis/claima"),
    ],
)
def test_project_folder_follows_env_then_key(tmp_path, monkeypatch, key, env, expected, subpath):
    """Fails if the active project is not the first of the installed ones, if AQUA_PROJECT does not win over it,
    or the folder is not the project subfolder."""
    if env is None:
        monkeypatch.delenv("AQUA_PROJECT", raising=False)
    else:
        monkeypatch.setenv("AQUA_PROJECT", env)
    context = ConfigContext(configdir=_project_install(tmp_path, project=key))

    assert context.project == expected
    assert context.get_project_folder("analysis") == os.path.join(str(tmp_path), subpath)


@pytest.mark.aqua
@pytest.mark.parametrize("key, installed", [("diagnostics", "diagnostics"), ("", "none")])
def test_project_folder_refuses_project_not_listed(tmp_path, monkeypatch, key, installed):
    """Fails if AQUA_PROJECT selects a project that is not listed as installed, even with its folder there."""
    monkeypatch.setenv("AQUA_PROJECT", "claima")
    context = ConfigContext(configdir=_project_install(tmp_path, project=key))

    assert context.projects == ([key] if key else [])
    with pytest.raises(FileNotFoundError, match=rf"Project 'claima' is not installed \(installed: {installed}\)"):
        context.get_project_folder("analysis")


@pytest.mark.aqua
@pytest.mark.parametrize(
    "project, env, match",
    [
        ("climatedt", None, r"tools folder for project 'climatedt' in .*tools/climatedt"),
        (None, None, r"No project set in .*config-aqua.yaml: this installation predates project support, run `aqua install`"),
        (None, "claima", r"No project set in .*config-aqua.yaml: this installation predates project support"),
    ],
)
def test_project_folder_refuses_missing_folder_and_flat_layout(tmp_path, monkeypatch, project, env, match):
    """Fails if a missing folder falls back to another project or the flat layout, if AQUA_PROJECT gives a project
    to an installation without project key, or if the error is not explicit."""
    if env is None:
        monkeypatch.delenv("AQUA_PROJECT", raising=False)
    else:
        monkeypatch.setenv("AQUA_PROJECT", env)
    # tools/ exists flat, but there is no tools/<project>: neither layout falls back to it
    context = ConfigContext(configdir=_project_install(tmp_path, project=project))

    assert context.project == project
    with pytest.raises(FileNotFoundError, match=match):
        context.get_project_folder("tools")


@pytest.mark.aqua
def test_project_folder_with_empty_key_does_not_suggest_reinstalling(tmp_path, monkeypatch):
    """Fails if an installation whose last project was removed (empty key) is told to run `aqua install`, which
    would wipe it, instead of adding or selecting a project."""
    monkeypatch.delenv("AQUA_PROJECT", raising=False)
    _project_install(tmp_path)
    dump_yaml(os.path.join(tmp_path, "config-aqua.yaml"), {"machine": "ci", "project": None})
    context = ConfigContext(configdir=str(tmp_path))

    assert context.has_project_layout()
    with pytest.raises(FileNotFoundError, match=r"No active project in .*`aqua project add <name>`") as excinfo:
        context.get_project_folder("analysis")
    assert "predates" not in str(excinfo.value)


@pytest.mark.aqua
@pytest.mark.parametrize("name", ["catgen", "fixes", "analysis/diagnostics"])
def test_project_folder_rejects_non_project_folders(tmp_path, name):
    """Fails if a folder that is not project-scoped (catgen included) is accepted."""
    context = ConfigContext(configdir=_project_install(tmp_path, project="diagnostics"))

    with pytest.raises(ValueError, match=f"'{name}' is not a project folder"):
        context.get_project_folder(name)

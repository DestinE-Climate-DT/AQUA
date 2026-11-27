"""Fixtures scoped to the AQUA console test suite."""

import io
import os
import shutil
import sys
from unittest.mock import patch

import fsspec
import pytest
from fsspec.implementations.dirfs import DirFileSystem
from utils_tests import AQUA_TESTS

from aqua.core.console.main import AquaConsole
from aqua.core.util import dump_yaml


@pytest.fixture(scope="session")
def catalog_copy_path():
    """Return the repository-local catalog used by console tests."""
    path = os.path.join(AQUA_TESTS, "catalog_copy")
    if not os.path.isdir(path):
        pytest.fail(f"Required console test catalog not found: {path}", pytrace=False)
    return path


@pytest.fixture(autouse=True)
def _restore_environ():
    """Restore the environment after each console test: commands such as `aqua analysis` export AQUA_* variables."""
    with patch.dict(os.environ):
        yield


class _FakeGitHub:
    """What the tests can read from, and ask to, the local stand-in of GitHub."""

    def __init__(self):
        self.repositories = []  # the user/repo of every access, in order
        self.fail = None  # an exception to raise when the files are downloaded, after their folders are created


class _LocalGitHubFileSystem(DirFileSystem):
    """Directory exposed as the fsspec GitHub filesystem, whose `ls` returns plain paths by default."""

    state = None

    def ls(self, path, detail=False, **kwargs):
        return super().ls(path, detail=detail, **kwargs)

    def get(self, *args, **kwargs):
        if self.state is not None and self.state.fail is not None:
            raise self.state.fail
        return super().get(*args, **kwargs)


@pytest.fixture(scope="session")
def fake_github_repo(tmp_path_factory, request):
    """A local stand-in of the Climate-DT-catalog repository: the installed `ci` catalog and two empty catalogs.

    The `ci` catalog is the one of the session-private configuration (see tests/utils_tests/config.py).
    """
    root = tmp_path_factory.mktemp("fake_github")
    shutil.copytree(os.path.join(request.config._aqua_private_config, "catalogs", "ci"), root / "catalogs" / "ci")
    for name in ("obs", "climatedt-gen2"):
        catalog = root / "catalogs" / name
        catalog.mkdir(parents=True)
        (catalog / "catalog.yaml").write_text("sources: {}\n")
        (catalog / "machine.yaml").write_text("{}\n")
    return root


@pytest.fixture
def fake_github(monkeypatch, fake_github_repo):
    """Serve the GitHub catalog repository from a local directory.

    The fsspec GitHub client is the only thing replaced; the token is removed so that the console takes the
    unauthenticated fsspec path. The real GitHub access is covered by the `network` test in test_console_github.py.
    It returns the repositories asked for, and can make the download fail (see `_FakeGitHub`).
    """
    real_filesystem = fsspec.filesystem
    state = _FakeGitHub()

    def filesystem(protocol, *args, **kwargs):
        if protocol == "github":
            state.repositories.append(f"{kwargs.get('org')}/{kwargs.get('repo')}")
            github = _LocalGitHubFileSystem(path=str(fake_github_repo), fs=real_filesystem("file"))
            github.state = state
            return github
        return real_filesystem(protocol, *args, **kwargs)

    monkeypatch.setattr(fsspec, "filesystem", filesystem)
    monkeypatch.delenv("GITHUB_TOKEN", raising=False)
    return state


def _hide_aqua_config():
    """Remove AQUA_CONFIG and return its value.

    Tests that install AQUA in a temporary HOME must not see the session-private AQUA_CONFIG: it takes precedence
    over HOME and the private configuration would be modified or removed.
    """
    return os.environ.pop("AQUA_CONFIG", None)


def _restore_aqua_config(value):
    """Restore AQUA_CONFIG as returned by `_hide_aqua_config`."""
    if value is None:
        os.environ.pop("AQUA_CONFIG", None)
    else:
        os.environ["AQUA_CONFIG"] = value


def _set_aqua_args(args):
    """Set command-line arguments for an in-process AQUA console invocation."""
    sys.argv = ["aqua"] + args


@pytest.fixture
def set_home():
    """Fixture to temporarily modify the HOME environment variable."""
    original_value = os.environ.get("HOME")
    original_config = _hide_aqua_config()

    def _modify_home(new_value):
        os.environ["HOME"] = str(new_value)

    yield _modify_home
    _restore_aqua_config(original_config)
    if original_value is not None:
        os.environ["HOME"] = original_value
    else:
        os.environ.pop("HOME", None)


@pytest.fixture
def delete_home():
    """Fixture to temporarily remove the HOME environment variable."""
    original_value = os.environ.get("HOME")
    original_config = _hide_aqua_config()

    def _modify_home():
        os.environ.pop("HOME", None)

    yield _modify_home
    _restore_aqua_config(original_config)
    if original_value is not None:
        os.environ["HOME"] = original_value
    else:
        os.environ.pop("HOME", None)


@pytest.fixture(scope="session")
def run_aqua_console_with_input():
    """Run AQUA console with interactive input."""

    def _run_aqua_console(args, input_text):
        old_argv = sys.argv
        old_stdin = sys.stdin
        try:
            _set_aqua_args(args)
            content = input_text if input_text.endswith("\n") else input_text + "\n"
            sys.stdin = io.StringIO(content)
            AquaConsole().execute()
        finally:
            sys.stdin = old_stdin
            sys.argv = old_argv

    return _run_aqua_console


@pytest.fixture
def project_install(tmp_path, monkeypatch):
    """Return a function building a minimal installation under tmp_path, found through AQUA_CONFIG.

    `project` is the value of the project key, a name or the list of the installed projects with the active one first:
    by default `diagnostics` is active and `claima` is installed too. `project=None` writes no project key
    (pre-project layout), `project=""` an empty one (no project installed), `folders` replaces the default project folders.
    Returns the path of config-aqua.yaml.
    """
    monkeypatch.delenv("AQUA_PROJECT", raising=False)

    def _build(
        project=("diagnostics", "claima"), folders=("analysis/diagnostics", "collections/diagnostics", "collections/claima")
    ):
        config = {"machine": "ci", "catalog": None}
        if project is not None:
            config["project"] = (list(project) if isinstance(project, tuple) else project) or None
        dump_yaml(os.path.join(tmp_path, "config-aqua.yaml"), config)
        for folder in (*folders, "catalogs"):
            os.makedirs(os.path.join(tmp_path, folder), exist_ok=True)
        monkeypatch.setenv("AQUA_CONFIG", str(tmp_path))
        return os.path.join(tmp_path, "config-aqua.yaml")

    return _build


@pytest.fixture(scope="session")
def run_aqua():
    """Run an AQUA console command in-process and return its exit status."""

    def _run_aqua_console(args):
        old_argv = sys.argv
        try:
            _set_aqua_args(args)
            return AquaConsole().execute()
        finally:
            sys.argv = old_argv

    return _run_aqua_console


@pytest.fixture(scope="class")
def aqua_install(request, tmp_path_factory, run_aqua, run_aqua_console_with_input):
    """Create and clean up one isolated AQUA installation per test class."""
    mydir = str(tmp_path_factory.mktemp(f"aqua_{request.cls.__name__}"))
    original_home = os.environ.get("HOME")
    original_config = _hide_aqua_config()
    os.environ["HOME"] = mydir

    try:
        run_aqua(["install", "github"])
        yield mydir
    finally:
        try:
            if os.path.exists(os.path.join(mydir, ".aqua")):
                run_aqua_console_with_input(["uninstall"], "yes")
        finally:
            _restore_aqua_config(original_config)
            if original_home is not None:
                os.environ["HOME"] = original_home
            else:
                os.environ.pop("HOME", None)

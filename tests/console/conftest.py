"""Fixtures scoped to the AQUA console test suite."""

import io
import os
import sys

import pytest
from utils_tests import APPROX_REL as APPROX_REL
from utils_tests import DPI as DPI
from utils_tests import LOGLEVEL as LOGLEVEL

from aqua.core.console.main import AquaConsole


def _set_aqua_args(args):
    """Set command-line arguments for an in-process AQUA console invocation."""
    sys.argv = ["aqua"] + args


@pytest.fixture
def set_home():
    """Fixture to temporarily modify the HOME environment variable."""
    original_value = os.environ.get("HOME")

    def _modify_home(new_value):
        os.environ["HOME"] = str(new_value)

    yield _modify_home
    if original_value is not None:
        os.environ["HOME"] = original_value
    else:
        os.environ.pop("HOME", None)


@pytest.fixture
def delete_home():
    """Fixture to temporarily remove the HOME environment variable."""
    original_value = os.environ.get("HOME")

    def _modify_home():
        os.environ.pop("HOME", None)

    yield _modify_home
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


@pytest.fixture(scope="session")
def run_aqua():
    """Run an AQUA console command in-process."""

    def _run_aqua_console(args):
        old_argv = sys.argv
        try:
            _set_aqua_args(args)
            AquaConsole().execute()
        finally:
            sys.argv = old_argv

    return _run_aqua_console


@pytest.fixture(scope="class")
def aqua_install(request, tmp_path_factory, run_aqua, run_aqua_console_with_input):
    """Create and clean up one isolated AQUA installation per test class."""
    mydir = str(tmp_path_factory.mktemp(f"aqua_{request.cls.__name__}"))
    original_home = os.environ.get("HOME")
    os.environ["HOME"] = mydir

    try:
        run_aqua(["install", "github"])
        yield mydir
    finally:
        try:
            if os.path.exists(os.path.join(mydir, ".aqua")):
                run_aqua_console_with_input(["uninstall"], "yes")
        finally:
            if original_home is not None:
                os.environ["HOME"] = original_home
            else:
                os.environ.pop("HOME", None)

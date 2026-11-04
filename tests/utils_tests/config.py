"""
Private AQUA configuration for the test session.

Tests must never read or modify the AQUA configuration installed on the machine
(``~/.aqua``, ``AQUA_CONFIG``): DROP writes catalog entries, the grid builder writes grid
files, config tests copy files into it. The session therefore works on a throw-away copy.
"""

import shutil
import sys
import tempfile
from pathlib import Path

from aqua.core.configurer import ConfigContext
from aqua.core.util import dump_yaml

AQUA_TESTS = Path(__file__).resolve().parents[2] / "AQUA_tests"

# configuration folders shipped with the package under test (and not with the installation)
PACKAGE_CONFIG_FOLDERS = ("catgen", "data_model", "fixes", "grids", "styles")


def make_private_config(prefix="aqua_pytest_config_"):
    """Create a private copy of the installed AQUA configuration and return its path.

    * ``config-aqua.yaml``, the catalogs and any other installed folder come from the installation.
      Linked folders are copied (not linked), so that tests can edit them without touching the original;
    * data models, fixes, grids and styles come from the ``aqua.core`` package that is imported,
      i.e. from the code under test and not from a possibly stale installation;
    * ``paths`` point to ``AQUA_tests`` as in CI, so that code without a catalog machine block
      does not write weights and areas in the current directory.

    The installed configuration is found as usual (``AQUA_CONFIG`` first, then ``$HOME/.aqua``).

    Args:
        prefix (str): prefix of the temporary directory name.

    Returns:
        str: path of the private configuration directory, to be exported as ``AQUA_CONFIG``.
    """
    installed = ConfigContext()
    source = Path(installed.configdir)
    package_config = Path(installed.packages.aqua_core_path) / "config"

    # short path on macOS, as the per-worker TMPDIR in conftest: it may end up in AF_UNIX socket paths
    target = Path(tempfile.mkdtemp(prefix=prefix, dir="/tmp" if sys.platform == "darwin" else None))

    config = dict(installed.config_dict)
    config["paths"] = {
        "grids": str(AQUA_TESTS / "grids"),
        "weights": str(AQUA_TESTS / "weights"),
        "areas": str(AQUA_TESTS / "weights"),
    }
    dump_yaml(str(target / "config-aqua.yaml"), config)

    for folder in PACKAGE_CONFIG_FOLDERS:
        shutil.copytree(package_config / folder, target / folder)

    for entry in source.iterdir():
        if entry.name in ("config-aqua.yaml", "catalogs", *PACKAGE_CONFIG_FOLDERS):
            continue
        if entry.is_dir():
            shutil.copytree(entry, target / entry.name)
        else:
            shutil.copy2(entry, target / entry.name)

    for catalog in config.get("catalog", []):
        if (source / "catalogs" / catalog).is_dir():
            shutil.copytree(source / "catalogs" / catalog, target / "catalogs" / catalog)

    return str(target)

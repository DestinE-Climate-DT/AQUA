"""
Private AQUA configuration for the test session.

Tests must never read or modify the AQUA configuration installed on the machine
(``~/.aqua``, ``AQUA_CONFIG``): DROP writes catalog entries, the grid builder writes grid
files, config tests copy files into it. The session therefore works on a throw-away copy.
"""

import os
import shutil
import sys
import tempfile

from aqua.core.configurer import ConfigContext
from aqua.core.util import dump_yaml

REPO_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__))))
AQUA_TESTS = os.path.join(REPO_ROOT, "AQUA_tests")

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
    source = installed.configdir
    package_config = os.path.join(installed.packages.aqua_core_path, "config")

    # short path on macOS, as the per-worker TMPDIR in conftest: it may end up in AF_UNIX socket paths
    target = tempfile.mkdtemp(prefix=prefix, dir="/tmp" if sys.platform == "darwin" else None)

    config = dict(installed.config_dict)
    config["paths"] = {
        "grids": os.path.join(AQUA_TESTS, "grids"),
        "weights": os.path.join(AQUA_TESTS, "weights"),
        "areas": os.path.join(AQUA_TESTS, "weights"),
    }
    dump_yaml(os.path.join(target, "config-aqua.yaml"), config)

    for folder in PACKAGE_CONFIG_FOLDERS:
        shutil.copytree(os.path.join(package_config, folder), os.path.join(target, folder))

    for name in os.listdir(source):
        if name in ("config-aqua.yaml", "catalogs", *PACKAGE_CONFIG_FOLDERS):
            continue
        entry = os.path.join(source, name)
        if os.path.isdir(entry):
            shutil.copytree(entry, os.path.join(target, name))
        else:
            shutil.copy2(entry, os.path.join(target, name))

    for catalog in config.get("catalog", []):
        catalog_dir = os.path.join(source, "catalogs", catalog)
        if os.path.isdir(catalog_dir):
            shutil.copytree(catalog_dir, os.path.join(target, "catalogs", catalog))

    return target

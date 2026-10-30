"""
Helpers to call the CDO executable.

The executable is resolved and version-checked through smmregrid, so AQUA and smmregrid
always use the same CDO: explicit argument, then the ``CDO`` environment variable, then ``cdo``
from the PATH. On Linux CDO is shipped with the conda-forge smmregrid package, on macOS
it must be installed with Homebrew.

Example:
    >>> from aqua.core.util import get_cdo, run_cdo
    >>> cdo = get_cdo()
    >>> run_cdo(["-f", "nc4", "copy", "in.nc", "out.nc"])
"""

import shutil
import subprocess

from smmregrid import check_cdo_version
from smmregrid.cdobinary import resolve_cdo


def get_cdo(cdo=None):
    """Return the path to the CDO executable, checking that its version is supported by smmregrid.

    Args:
        cdo (str, optional): CDO executable to use. Defaults to None, i.e. the ``CDO``
            environment variable or ``cdo`` from the PATH.

    Returns:
        str: The path to the CDO executable.

    Raises:
        FileNotFoundError: If the CDO executable is not found.
        RuntimeError: If the CDO version is older than the one required by smmregrid.
    """
    cdo = resolve_cdo(cdo)
    check_cdo_version(cdo)
    return shutil.which(cdo) or cdo


def run_cdo(args, cdo=None):
    """Run CDO with the given arguments.

    Args:
        args (list): Arguments passed to CDO, e.g. ``["-f", "nc4", "copy", "in.nc", "out.nc"]``.
        cdo (str, optional): CDO executable to use. Defaults to None (see ``get_cdo``).

    Raises:
        RuntimeError: If the CDO command fails, with the CDO error message.
    """
    command = [get_cdo(cdo), *args]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    if result.returncode != 0:
        raise RuntimeError(f"CDO command '{' '.join(command)}' failed: {result.stderr.strip()}")

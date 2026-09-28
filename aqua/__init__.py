"""AQUA core package - provides core functionality"""

import warnings

# Extend namespace to allow aqua-diagnostics to contribute
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# HACK: ignore intake warning when parsing catalog YAMLs with substitutions
warnings.filterwarnings("ignore", message="Shell command not executed due to getshell=False")

from .core import *  # noqa: E402, F403
from .core import __all__, __version__  # noqa: E402, F401

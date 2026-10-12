"""AQUA core package - provides core functionality"""

import warnings

# Extend namespace to allow aqua-diagnostics to contribute
__path__ = __import__("pkgutil").extend_path(__path__, __name__)

# HACK: ignore intake warning when parsing catalog YAMLs with substitutions
warnings.filterwarnings("ignore", message="Shell command not executed due to getshell=False")

from .core import __all__, __version__  # noqa: E402, F401
from .core import resolve_lazy as _resolve_lazy  # noqa: E402


def __getattr__(name):
    # resolve through aqua.core's lazy table, not getattr(aqua.core, ...): once a submodule such as
    # aqua.core.histogram is imported, the package attribute holds the submodule instead of the function
    if name in __all__:
        value = _resolve_lazy(name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")


def __dir__():
    return sorted(set(globals()) | set(__all__))

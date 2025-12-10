"""AQUA core package - provides core functionality"""

# Extend namespace to allow aqua-diagnostics to contribute
__path__ = __import__('pkgutil').extend_path(__path__, __name__)

from .core import *
from .core import __version__, __all__

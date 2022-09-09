from ._version import get_versions

__version__ = get_versions()["version"]
del get_versions

from . import event
from . import io
from . import regrid
from . import blocked
from . import profile

from .blocked import blocked_resample, blocked_groupby, approx_percentile

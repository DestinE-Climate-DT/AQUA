import intake  # Import this first to avoid circular imports during discovery.

# from intake.container import register_container
from .fdb import IntakeFDBSource, open_gsv, open_polytope
from .icechunk import IcechunkSource

__all__ = ["IntakeFDBSource", "IcechunkSource", "open_gsv", "open_polytope"]

try:
    intake.registry.drivers.register_driver('gsv', IntakeFDBSource)
except ValueError:
    pass

try:
    intake.registry.drivers.register_driver('icechunk', IcechunkSource)
except ValueError:
    pass

## register_container('gsv', GSVSource)

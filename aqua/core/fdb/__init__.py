import intake  # Import this first to avoid circular imports during discovery.
# from intake.container import register_container

from .fdb import IntakeFDBSource

try:
     intake.registry.drivers.register_driver('gsv', IntakeFDBSource)
except ValueError:
     pass

## register_container('gsv', GSVSource)

import intake  # Import this first to avoid circular imports during discovery.
# from intake.container import register_container

from .open_gsv import open_gsv
from .gsv import IntakeGSVSource

try:
     intake.registry.drivers.register_driver('gsv', IntakeGSVSource)
except ValueError:
     print("THE HORROR! the gsv driver is already registered.")
     pass

## register_container('gsv', GSVSource)

import intake  # Import this first to avoid circular imports during discovery.
# from intake.container import register_container

from .gsv import IntakeGSVSource

try:
     intake.registry.drivers.register_driver('gsv', IntakeGSVSource)
except ValueError:
     pass

## register_container('gsv', GSVSource)

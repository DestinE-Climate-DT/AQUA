import intake  # Import this first to avoid circular imports during discovery.
# from intake.container import register_container

from .intake_gsv import GSVSource

try:
    intake.registry.drivers.register_driver('gsv', GSVSource)
except ValueError:
    pass

# register_container('gsv', GSVSource)

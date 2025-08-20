import intake
from .intake_odb import ODBSource

try:
    intake.registry.drivers.register_driver('odb', ODBSource)
except ValueError:
    pass
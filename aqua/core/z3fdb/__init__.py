import intake  # Import this first to avoid circular imports during discovery.

from .intake_z3fdb import Z3FDBSource

try:
    intake.registry.drivers.register_driver("z3fdb", Z3FDBSource)
except ValueError:
    pass

import intake  # Import this first to avoid circular imports during discovery.

# from intake.container import register_container
from .fdb import IntakeFDBSource, open_gsv, open_polytope
from .icechunk import IntakeIcechunkSource
from .xarray import IntakeNetCDFSource, IntakeZarrSource, install_intake_xarray_stub

__all__ = [
    "IntakeFDBSource",
    "IntakeIcechunkSource",
    "IntakeNetCDFSource",
    "IntakeZarrSource",
    "open_gsv",
    "open_polytope",
]

try:
    intake.registry.drivers.register_driver('gsv', IntakeFDBSource)
except ValueError:
    pass

try:
    intake.registry.drivers.register_driver('icechunk', IntakeIcechunkSource)
except ValueError:
    pass

# clobber=True: the 'netcdf' and 'zarr' names are claimed by the intake-xarray
# entry points when that package is still installed, and runtime registration
# must win over them. It also makes the call idempotent (no ValueError on re-import).
intake.registry.drivers.register_driver('netcdf', IntakeNetCDFSource, clobber=True)
intake.registry.drivers.register_driver('zarr', IntakeZarrSource, clobber=True)

# Legacy catalogs may still import intake_xarray through a 'plugins' block:
# when the real package is absent, provide a stub mapping to the AQUA sources.
install_intake_xarray_stub()

## register_container('gsv', GSVSource)

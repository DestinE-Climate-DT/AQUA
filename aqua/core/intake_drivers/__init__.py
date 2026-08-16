import intake  # Import this first to avoid circular imports during discovery.

# from intake.container import register_container
from .fdb import IntakeFDBSource, open_gsv, open_polytope, open_z3fdb
from .icechunk import IntakeIcechunkSource
from .xarray import IntakeNetCDFSource, IntakeZarrSource, install_intake_xarray_stub

__all__ = [
    "IntakeFDBSource",
    "IntakeIcechunkSource",
    "IntakeNetCDFSource",
    "IntakeZarrSource",
    "install_intake_xarray_stub",
    "open_gsv",
    "open_polytope",
    "open_z3fdb",
]

try:
    intake.registry.drivers.register_driver('gsv', IntakeFDBSource)
except ValueError:
    pass

try:
    intake.registry.drivers.register_driver('icechunk', IntakeIcechunkSource)
except ValueError:
    pass

# clobber=True (no try/except like above): while intake-xarray is still installed
# in an environment, its entry points own the 'netcdf' and 'zarr' driver names and
# the AQUA sources must win over them (the backend relies on their exposed
# .data/.metadata/.xarray_kwargs attributes).
intake.registry.drivers.register_driver('netcdf', IntakeNetCDFSource, clobber=True)
intake.registry.drivers.register_driver('zarr', IntakeZarrSource, clobber=True)

# Legacy catalogs may still import intake_xarray through their 'plugins' block:
# when the real package is absent, install a stub mapping it to the AQUA sources.
install_intake_xarray_stub()

## register_container('gsv', GSVSource)

"""FDB source adapter built on the shared AQUA intake 2 adapter."""

from ..base import IntakeSourceAdapter


class IntakeFDBSourceAdapter(IntakeSourceAdapter):
    """
    Adapter exposing FDB-based intake 2 readers through the v1 DataSource interface.

    All the interface logic (``to_dask``, ``read``, ``read_chunked``, ``get``)
    is inherited from :class:`aqua.core.intake_drivers.base.IntakeSourceAdapter`.
    """

    container = "fdb"
    name = "fdb"

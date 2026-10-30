"""Time dimension name resolution."""


def resolve_time_dim(dim: str | None = None) -> str:
    """Time dimension name: the one given or the one of the default data model."""
    if dim is not None:
        return dim
    # Imported here: data_model imports configurer, which imports aqua.core.util
    from aqua.core.data_model import AquaCoordNames

    return AquaCoordNames().time

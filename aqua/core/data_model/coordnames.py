"""Coordinate names of a data model, resolved once and shared.

Example:
    >>> from aqua.core.data_model import AquaCoordNames
    >>> coords = AquaCoordNames()  # default 'aqua' data model
    >>> data.sel({coords.time: slice("2000", "2001")}).mean(coords.lon)
"""

from dataclasses import FrozenInstanceError

from aqua.core.default import (
    AQUA_DEPTH,
    AQUA_HEIGHT,
    AQUA_ISOBARIC,
    AQUA_LATITUDE,
    AQUA_LONGITUDE,
    AQUA_TIME,
)

from .coord_utils import scan_coord


class AquaCoordNames:
    """
    Immutable, cached set of the coordinate names used by a data model.

    Instances are shared: ``AquaCoordNames("aqua") is AquaCoordNames("aqua")``.

    Attributes:
        time, lat, lon, isobaric, depth, height (str): Coordinate names in the data model.
        data_model (str): Name of the data model they come from.

    Raises:
        FileNotFoundError: If the data model does not exist.
        KeyError: If the data model does not define one of the coordinates.
    """

    __slots__ = ("time", "lat", "lon", "isobaric", "depth", "height", "data_model")
    _instances = {}

    def __new__(cls, data_model: str = "aqua"):
        if data_model not in cls._instances:
            self = super().__new__(cls)
            for attr, internal in (
                ("time", AQUA_TIME),
                ("lat", AQUA_LATITUDE),
                ("lon", AQUA_LONGITUDE),
                ("isobaric", AQUA_ISOBARIC),
                ("depth", AQUA_DEPTH),
                ("height", AQUA_HEIGHT),
            ):
                object.__setattr__(self, attr, scan_coord(internal, data_model))
            object.__setattr__(self, "data_model", data_model)
            cls._instances[data_model] = self
        return cls._instances[data_model]

    def __setattr__(self, name, value):
        raise FrozenInstanceError(f"cannot assign to field '{name}'")

    def __delattr__(self, name):
        raise FrozenInstanceError(f"cannot delete field '{name}'")

    def __repr__(self):
        fields = ", ".join(f"{attr}={getattr(self, attr)!r}" for attr in self.__slots__)
        return f"AquaCoordNames({fields})"

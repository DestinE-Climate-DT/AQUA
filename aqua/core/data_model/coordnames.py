"""Coordinate names of a data model, resolved once and shared.

Example:
    >>> from aqua.core.data_model import aqua_coords
    >>> coords = aqua_coords()  # default 'aqua' data model
    >>> data.sel({coords.time: slice("2000", "2001")}).mean(coords.lon)
"""

from dataclasses import dataclass
from functools import cache

from aqua.core.default import (
    AQUA_DEPTH,
    AQUA_HEIGHT,
    AQUA_ISOBARIC,
    AQUA_LATITUDE,
    AQUA_LONGITUDE,
    AQUA_TIME,
)

from .coord_utils import scan_coord


@dataclass(frozen=True)
class CoordNames:
    """
    Immutable set of the coordinate names used by a data model.

    Attributes:
        time, lat, lon, isobaric, depth, height (str): Coordinate names in the data model.
        data_model (str): Name of the data model they come from.
    """

    time: str
    lat: str
    lon: str
    isobaric: str
    depth: str
    height: str
    data_model: str = "aqua"


@cache
def aqua_coords(data_model: str = "aqua") -> CoordNames:
    """
    Get the (cached) coordinate names of a data model.

    Args:
        data_model (str): Data model name. Default is 'aqua'.

    Returns:
        CoordNames: Frozen object with the coordinate names.
    """
    return CoordNames(
        time=scan_coord(AQUA_TIME, data_model),
        lat=scan_coord(AQUA_LATITUDE, data_model),
        lon=scan_coord(AQUA_LONGITUDE, data_model),
        isobaric=scan_coord(AQUA_ISOBARIC, data_model),
        depth=scan_coord(AQUA_DEPTH, data_model),
        height=scan_coord(AQUA_HEIGHT, data_model),
        data_model=data_model,
    )

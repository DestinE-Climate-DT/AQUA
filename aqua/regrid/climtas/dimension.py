#!/usr/bin/env python
# Copyright 2018 ARC Centre of Excellence for Climate Extremes
# author: Scott Wales <scott.wales@unimelb.edu.au>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from __future__ import print_function

from cfunits import Units
import numpy


def remove_degenerate_axes(coord):
    """
    Remove any degenerate axes from the coordinate, where all the values along a dimension are identical

    Args:
        coord (xarray.DataArray): Co-ordinate to operate on

    Returns:
        xarray.DataArray with degenerate axes removed
    """

    for d in coord.dims:
        if numpy.allclose(coord.max(dim=d) - coord.min(dim=d), 0):
            coord = coord.mean(dim=d)

    return coord


def identify_lat_lon(dataarray):
    """
    Identify the latitude and longitude dimensions of a dataarray using CF
    attributes

    Args:
        dataarray: Source dataarray

    Returns:
        (lat, lon): Tuple of `xarray.Dataarray` for the latitude and longitude
            dimensions

    Todo:
        * Assumes latitude and longitude are unique
    """

    lat = None
    lon = None

    for c in dataarray.coords.values():
        if (
            c.attrs.get("standard_name", "") == "latitude"
            or Units(c.attrs.get("units", "")).islatitude
            or c.attrs.get("axis", "") == "Y"
        ):
            lat = c

        if (
            c.attrs.get("standard_name", "") == "longitude"
            or Units(c.attrs.get("units", "")).islongitude
            or c.attrs.get("axis", "") == "X"
        ):
            lon = c

    if lat is None or lon is None:
        raise Exception("Couldn't identify horizontal coordinates")

    return (lat, lon)


def identify_time(dataarray):
    """
    Identify the time dimension of a dataarray using CF attributes

    Args:
        dataarray: Source dataarray

    Returns:
        :obj:`xarray.Dataarray` for the time dimension

    Todo:
        * Assumes time dimension is unique
    """

    for c in dataarray.coords.values():
        if (
            c.attrs.get("standard_name", "") == "time"
            or Units(c.attrs.get("units", "")).isreftime
            or Units(c.encoding.get("units", "")).isreftime
            or c.attrs.get("axis", "") == "T"
        ):
            return c

    raise Exception("No time axis found")

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
"""Dask-aware regridding

To apply a regridding you will need a set of weights mapping from the source
grid to the target grid.

Regridding weights can be generated online using ESMF_RegridWeightGen
(:func:`esmf_generate_weights`) or CDO (:func:`cdo_generate_weights`), or
offline by calling these programs externally (this is recommended especially
for large grids, using ESMF_REgridWeightGen in MPI mode).

Once calculated :func:`regrid` will apply these weights using a Dask sparse
matrix multiply, maintaining chunking in dimensions other than lat and lon.

:class:`Regrid` can create basic weights and store them to apply the weights to
multiple datasets.
"""

from .dimension import remove_degenerate_axes

from datetime import datetime
from shutil import which
import dask.array
import math
import os
import sparse
import subprocess
import sys
import tempfile
import xarray
import numpy


def cdo_generate_weights(
    source_grid,
    target_grid,
    method="bil",
    extrapolate=True,
    remap_norm="fracarea",
    remap_area_min=0.0,
):
    """
    Generate weights for regridding using CDO

    Available weight generation methods are:

     * bic: SCRIP Bicubic
     * bil: SCRIP Bilinear
     * con: SCRIP First-order conservative
     * con2: SCRIP Second-order conservative
     * dis: SCRIP Distance-weighted average
     * laf: YAC Largest area fraction
     * ycon: YAC First-order conservative
     * nn: Nearest neighbour

    Run ``cdo gen${method} --help`` for details of each method

    Args:
        source_grid (xarray.DataArray): Source grid
        target_grid (xarray.DataArray): Target grid
            description
        method (str): Regridding method
        extrapolate (bool): Extrapolate output field
        remap_norm (str): Normalisation method for conservative methods
        remap_area_min (float): Minimum destination area fraction

    Returns:
        :obj:`xarray.Dataset` with regridding weights
    """

    supported_methods = ["bic", "bil", "con", "con2", "dis", "laf", "nn", "ycon"]
    if method not in supported_methods:
        raise Exception
    if remap_norm not in ["fracarea", "destarea"]:
        raise Exception

    # Make some temporary files that we'll feed to CDO
    weight_file = tempfile.NamedTemporaryFile()

    if type(source_grid) == str:
        sgrid = source_grid
    else:
        source_grid_file = tempfile.NamedTemporaryFile()
        source_grid.to_netcdf(source_grid_file.name)
        sgrid = source_grid_file.name

    if type(target_grid) == str:
        tgrid = target_grid
    else:
        target_grid_file = tempfile.NamedTemporaryFile()
        target_grid.to_netcdf(target_grid_file.name)
        tgrid = target_grid_file.name

    # Setup environment
    env = os.environ
    if extrapolate:
        env["REMAP_EXTRAPOLATE"] = "on"
    else:
        env["REMAP_EXTRAPOLATE"] = "off"

    env["CDO_REMAP_NORM"] = remap_norm
    env["REMAP_AREA_MIN"] = "%f" % (remap_area_min)

    try:
        # Run CDO
        subprocess.check_output(
            [
                "cdo",
                "gen%s,%s" % (method, tgrid),
                sgrid,
                weight_file.name,
            ],
            stderr=subprocess.PIPE,
            env=env,
        )

        # Grab the weights file it outputs as a xarray.Dataset
        weights = xarray.open_dataset(weight_file.name, engine="netcdf4")
        return weights

    except subprocess.CalledProcessError as e:
        # Print the CDO error message
        print(e.output.decode(), file=sys.stderr)
        raise

    finally:
        # Clean up the temporary files
        if not type(source_grid) == str:
            source_grid_file.close()
        if not type(target_grid) == str:
            target_grid_file.close()
        weight_file.close()


def esmf_generate_weights(
    source_grid,
    target_grid,
    method="bilinear",
    extrap_method="nearestidavg",
    norm_type="dstarea",
    line_type=None,
    pole=None,
    ignore_unmapped=False,
):
    """Generate regridding weights with ESMF

    https://www.earthsystemcog.org/projects/esmf/regridding

    Args:
        source_grid (:obj:`xarray.Dataarray`): Source grid. If masked the mask
            will be used in the regridding
        target_grid (:obj:`xarray.Dataarray`): Target grid. If masked the mask
            will be used in the regridding
        method (str): ESMF Regridding method, see ``ESMF_RegridWeightGen --help``
        extrap_method (str): ESMF Extrapolation method, see ``ESMF_RegridWeightGen --help``

    Returns:
        :obj:`xarray.Dataset` with regridding information from
            ESMF_RegridWeightGen
    """
    # Make some temporary files that we'll feed to ESMF
    source_file = tempfile.NamedTemporaryFile(suffix=".nc")
    target_file = tempfile.NamedTemporaryFile(suffix=".nc")
    weight_file = tempfile.NamedTemporaryFile(suffix=".nc")

    rwg = "ESMF_RegridWeightGen"

    if "_FillValue" not in source_grid.encoding:
        source_grid.encoding["_FillValue"] = -1e20

    if "_FillValue" not in target_grid.encoding:
        target_grid.encoding["_FillValue"] = -1e20

    try:
        source_grid.to_netcdf(source_file.name)
        target_grid.to_netcdf(target_file.name)

        command = [
            rwg,
            "--source",
            source_file.name,
            "--destination",
            target_file.name,
            "--weight",
            weight_file.name,
            "--method",
            method,
            "--extrap_method",
            extrap_method,
            "--norm_type",
            norm_type,
            #'--user_areas',
            "--no-log",
            "--check",
        ]

        if isinstance(source_grid, xarray.DataArray):
            command.extend(["--src_missingvalue", source_grid.name])
        if isinstance(target_grid, xarray.DataArray):
            command.extend(["--dst_missingvalue", target_grid.name])
        if ignore_unmapped:
            command.extend(["--ignore_unmapped"])
        if line_type is not None:
            command.extend(["--line_type", line_type])
        if pole is not None:
            command.extend(["--pole", pole])

        out = subprocess.check_output(args=command, stderr=subprocess.PIPE)
        print(out.decode("utf-8"))

        weights = xarray.open_dataset(weight_file.name, engine="netcdf4")
        # Load so we can delete the temp file
        return weights.load()

    except subprocess.CalledProcessError as e:
        print(e)
        print(e.output.decode("utf-8"))
        raise

    finally:
        # Clean up the temporary files
        source_file.close()
        target_file.close()
        weight_file.close()


def compute_weights_matrix(weights):
    """
    Convert the weights from CDO/ESMF to a numpy array
    """
    w = weights
    if w.title.startswith("ESMF"):
        # ESMF style weights
        src_address = w.col - 1
        dst_address = w.row - 1
        remap_matrix = w.S
        w_shape = (w.sizes["n_a"], w.sizes["n_b"])

    else:
        # CDO style weights
        src_address = w.src_address - 1
        dst_address = w.dst_address - 1
        remap_matrix = w.remap_matrix[:, 0]
        w_shape = (w.sizes["src_grid_size"], w.sizes["dst_grid_size"])

    # Create a sparse array from the weights
    sparse_weights_delayed = dask.delayed(sparse.COO)(
        [src_address.data, dst_address.data], remap_matrix.data, shape=w_shape
    )
    sparse_weights = dask.array.from_delayed(
        sparse_weights_delayed, shape=w_shape, dtype=remap_matrix.dtype
    )

    return sparse_weights


def apply_weights(source_data, weights, weights_matrix=None):
    """
    Apply the CDO weights ``weights`` to ``source_data``, performing a regridding operation

    Args:
        source_data (xarray.Dataset): Source dataset
        weights (xarray.Dataset): CDO weights information

    Returns:
        xarray.Dataset: Regridded version of the source dataset
    """
    # Alias the weights dataset from CDO
    w = weights

    # The weights file contains a sparse matrix, that we need to multiply the
    # source data's horizontal grid with to get the regridded data.
    #
    # A bit of messing about with `.stack()` is needed in order to get the
    # dimensions to conform - the horizontal grid needs to be converted to a 1d
    # array, multiplied by the weights matrix, then unstacked back into a 2d
    # array

    if w.title.startswith("ESMF"):
        # ESMF style weights
        src_address = w.col - 1
        dst_address = w.row - 1
        remap_matrix = w.S
        w_shape = (w.sizes["n_a"], w.sizes["n_b"])

        dst_grid_shape = w.dst_grid_dims.values
        dst_grid_center_lat = w.yc_b.data.reshape(dst_grid_shape[::-1])
        dst_grid_center_lon = w.xc_b.data.reshape(dst_grid_shape[::-1])

        dst_mask = w.mask_b

        axis_scale = 1  # Weight lat/lon in degrees

    else:
        # CDO style weights
        src_address = w.src_address - 1
        dst_address = w.dst_address - 1
        remap_matrix = w.remap_matrix[:, 0]
        w_shape = (w.sizes["src_grid_size"], w.sizes["dst_grid_size"])

        dst_grid_shape = w.dst_grid_dims.values
        dst_grid_center_lat = w.dst_grid_center_lat.data.reshape(
            #dst_grid_shape[::-1], order="C"
            dst_grid_shape[::-1]
        )
        dst_grid_center_lon = w.dst_grid_center_lon.data.reshape(
            # dst_grid_shape[::-1], order="C"
            dst_grid_shape[::-1]
        )

        dst_mask = w.dst_grid_imask

        axis_scale = 180.0 / math.pi  # Weight lat/lon in radians

    # Check lat/lon are the last axes
    #source_lat, source_lon = identify_lat_lon(source_data)
    #if not (
        #source_lat.name in source_data.dims[-2:]
        #and source_lon.name in source_data.dims[-2:]
    #):
    #    raise Exception(
    #        "Last two dimensions should be spatial coordinates,"
    #        f" got {source_data.dims[-2:]}"
    #    )

    # Find dimensions to keep
    nd = sum([(d not in ['i', 'j', 'x', 'y', 'lon', 'lat', 'longitude', 'latitude', 'ncells', 'values']) for d in source_data.dims])

    kept_shape = list(source_data.shape[0:nd])
    kept_dims = list(source_data.dims[0:nd])

    if weights_matrix is None:
        weights_matrix = compute_weights_matrix(weights)

    # Remove the spatial axes, apply the weights, add the spatial axes back
    source_array = source_data.data
    if isinstance(source_array, dask.array.Array):
        source_array = dask.array.reshape(source_array, kept_shape + [-1])
    else:
        source_array = numpy.reshape(source_array, kept_shape + [-1])

    # Handle input mask
    dask.array.ma.set_fill_value(source_array, 1e20)
    source_array = dask.array.ma.fix_invalid(source_array)
    source_array = dask.array.ma.filled(source_array)

    target_dask = dask.array.tensordot(source_array, weights_matrix, axes=1)

    bmask = numpy.broadcast_to(
        dst_mask.data.reshape([1 for d in kept_shape] + [-1]), target_dask.shape
    )

    target_dask = dask.array.where(bmask != 0.0, target_dask, numpy.nan)
    target_dask = dask.array.reshape(
        target_dask, kept_shape + [dst_grid_shape[1], dst_grid_shape[0]]
    )

    # Create a new DataArray for the output
    target_da = xarray.DataArray(
        target_dask,
        dims=kept_dims + ["i", "j"],
        coords={
            k: v
            for k, v in source_data.coords.items()
            if set(v.dims).issubset(kept_dims)
        },
        name=source_data.name,
    )
    target_da.coords["lat"] = xarray.DataArray(dst_grid_center_lat, dims=["i", "j"])
    target_da.coords["lon"] = xarray.DataArray(dst_grid_center_lon, dims=["i", "j"])

    # Clean up coordinates
    target_da.coords["lat"] = remove_degenerate_axes(target_da.lat)
    target_da.coords["lon"] = remove_degenerate_axes(target_da.lon)

    # Convert to degrees if needed
    target_da.coords["lat"] = target_da.lat * axis_scale
    target_da.coords["lon"] = target_da.lon * axis_scale

    # If a regular grid drop the 'i' and 'j' dimensions
    if target_da.coords["lat"].ndim == 1 and target_da.coords["lon"].ndim == 1:
        target_da = target_da.rename({"i": "lat", "j": "lon"})

    # Add metadata to the coordinates
    target_da.coords["lat"].attrs["units"] = "degrees_north"
    target_da.coords["lat"].attrs["standard_name"] = "latitude"
    target_da.coords["lon"].attrs["units"] = "degrees_east"
    target_da.coords["lon"].attrs["standard_name"] = "longitude"

    # Now rename to the original coordinate names
    #target_da = target_da.rename({"lat": source_lat.name, "lon": source_lon.name})

    return target_da


class Regridder(object):
    """Set up the regridding operation

    Supply either both ``source_grid`` and ``dest_grid`` or just ``weights``.

    For large grids you may wish to pre-calculate the weights using
    ESMF_RegridWeightGen, if not supplied ``weights`` will be calculated from
    ``source_grid`` and ``dest_grid`` using CDO's genbil function.

    Weights may be pre-computed by an external program, or created using
    :func:`cdo_generate_weights` or :func:`esmf_generate_weights`

    Args:
        source_grid (:class:`coecms.grid.Grid` or :class:`xarray.DataArray`): Source grid / sample dataset
        target_grid (:class:`coecms.grid.Grid` or :class:`xarray.DataArray`): Target grid / sample dataset
        weights (:class:`xarray.Dataset`): Pre-computed interpolation weights
    """

    def __init__(self, source_grid=None, target_grid=None, weights=None):

        if (source_grid is None or target_grid is None) and weights is None:
            raise Exception(
                "Either weights or source_grid/target_grid must be supplied"
            )

        # Is there already a weights file?
        if weights is not None:
            self.weights = weights
        else:
            # Generate the weights with CDO
            _source_grid = identify_grid(source_grid)
            _target_grid = identify_grid(target_grid)
            self.weights = cdo_generate_weights(_source_grid, _target_grid)

        self.weights_matrix = compute_weights_matrix(self.weights)

    def regrid(self, source_data):
        """Regrid ``source_data`` to match the target grid

        Args:
            source_data (:class:`xarray.DataArray` or xarray.Dataset): Source
            variable

        Returns:
            :class:`xarray.DataArray` or xarray.Dataset with a regridded
            version of the source variable
        """

        if isinstance(source_data, xarray.Dataset):
            return source_data.apply(self.regrid)
        else:
            return apply_weights(
                source_data, self.weights, weights_matrix=self.weights_matrix
            )


def regrid(source_data, target_grid=None, weights=None):
    """
    A simple regrid. Inefficient if you are regridding more than one dataset
    to the target grid because it re-generates the weights each time you call
    the function.

    To save the weights use :class:`Regridder`.

    Args:
        source_data (:class:`xarray.DataArray`): Source variable
        target_grid (:class:`coecms.grid.Grid` or :class:`xarray.DataArray`): Target grid / sample variable

    Returns:
        :class:`xarray.DataArray` with a regridded version of the source variable
    """

    regridder = Regridder(source_data, target_grid=target_grid, weights=weights)

    return regridder.regrid(source_data)

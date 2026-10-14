"""Class to create a catalog entry for DROP"""

import glob
import math
import os

import xarray as xr
from dask.utils import parse_bytes
from smmregrid import GridType

# default grid name and chunk target size for DROP outputs, if not specified otherwise
from aqua.core.default import DEFAULT_DROP_CHUNK_TARGET, DEFAULT_DROP_GRID
from aqua.core.logger import log_configure
from aqua.core.util import format_realization, replace_intake_vars, replace_urlpath_jinja

from .output_path_builder import OutputPathBuilder


class CatalogEntryBuilder:
    """Class to create a catalog entry for DROP"""

    def __init__(
        self,
        catalog,
        model,
        exp,
        resolution,
        realization=None,
        frequency=None,
        stat=None,
        region=None,
        loglevel="WARNING",
        **kwargs,
    ):
        """
        Initialize the CatalogEntryBuilder with the necessary parameters.

        Args:
            catalog (str): Name of the catalog.
            model (str): Name of the model.
            exp (str): Name of the experiment.
            resolution (str): Resolution of the data.
            realization (str, optional): Realization name. Defaults to 'r1'.
            frequency (str, optional): Frequency of the data. Defaults to 'native'.
            stat (str, optional): Statistic type. Defaults to 'nostat'.
            region (str, optional): Region. Defaults to 'global'.
            loglevel (str, optional): Logging level. Defaults to 'WARNING'.
            **kwargs: Additional keyword arguments for flexibility.
        """

        self.catalog = catalog
        self.model = model
        self.exp = exp
        self.resolution = resolution

        # Set defaults if not provided
        self.realization = format_realization(realization)  # ensure realization is formatted correctly
        self.frequency = frequency if frequency is not None else "native"
        self.stat = stat if stat is not None else "nostat"
        self.region = region if region is not None else "global"

        self.kwargs = kwargs
        self.opt = OutputPathBuilder(
            catalog=catalog,
            model=model,
            exp=exp,
            realization=self.realization,
            resolution=self.resolution,
            frequency=self.frequency,
            stat=self.stat,
            region=self.region,
            **self.kwargs,
        )
        self.logger = log_configure(log_level=loglevel, log_name="CatalogEntryBuilder")
        self.loglevel = loglevel

    def create_entry_name(self, output_format="netcdf"):
        """
        Create an entry name for the DROP outputs
        """
        # Default LRA-100-monthly entry keeps the 'lra-' prefix
        if self.resolution == "r100" and self.frequency == "monthly":
            entry_name = f"lra-{self.resolution}-{self.frequency}"
        else:
            # All other entries drop the 'lra-' prefix
            entry_name = f"{self.resolution}-{self.frequency}"

        if output_format == "zarr":
            entry_name += "-zarr"

        return entry_name

    # def update_urlpath(self, oldpath, newpath):
    #     """Update the urlpath in the catalog entry."""
    #     old = to_list(oldpath)
    #     new = to_list(newpath)

    #     for n in new:
    #         if n not in old:
    #             old.append(n)

    #     return old if len(old) > 1 else old[0]

    def define_optimal_chunks(self, urlpath, output_format="netcdf", chunk_target=DEFAULT_DROP_CHUNK_TARGET):
        """Define the read chunks of DROP outputs from the files already on disk.

        The first file (sorted) of each variable is opened lazily (metadata only) to get dimension
        sizes and dtype. Horizontal dimensions are always kept whole, then time and vertical dimensions are filled
        until a chunk reaches ``chunk_target`` bytes. If a single horizontal slice exceeds the
        target, the chunk holds one slice. The time chunk cannot exceed the length
        of a single file, since xarray.open_mfdataset chunks each file separately.
        Chunks of different variables are merged keeping the smallest size along each dimension.

        Args:
            urlpath (str): Fully expanded glob path of the DROP outputs.
            output_format (str): Output format, 'netcdf' or 'zarr'. Defaults to 'netcdf'.
            chunk_target (str or int): Target chunk size, as a Dask size string (e.g. '128MiB', '0.2GiB')
                or a number of bytes. 'auto' leaves the chunking to Dask. Defaults to DEFAULT_DROP_CHUNK_TARGET.

        Returns:
            dict or str: Chunk size along each dimension, -1 for dimensions kept whole.
                Empty if no output file is found, 'auto' if chunk_target is 'auto'.
        """
        if chunk_target == "auto":
            self.logger.info("Chunks left to Dask auto chunking")
            return "auto"

        target = parse_bytes(chunk_target) if isinstance(chunk_target, str) else int(chunk_target)

        files = sorted(glob.glob(urlpath))
        if not files:
            self.logger.warning("No DROP output found in %s, leaving chunks undefined", urlpath)
            return {}

        # keep the first file of each variable. OutputPathBuilder.build_filename always puts the
        # date stamp (YYYY, YYYYMM, ...) as the last "_"-separated component, so stripping it with
        # rsplit leaves a key unique to each variable (and level), without opening the whole dataset
        samples = {}
        for filename in files:
            samples.setdefault(os.path.basename(filename).rsplit("_", 1)[0], filename)

        engine = "zarr" if output_format == "zarr" else None
        chunks = {}
        for sample in samples.values():
            with xr.open_dataset(sample, engine=engine, chunks={}) as ds:
                for var in ds.data_vars:
                    self._merge_chunks(chunks, self._variable_chunks(ds[var], target))

        self.logger.info("Chunks for target size %s: %s", chunk_target, chunks)
        return chunks

    def _variable_chunks(self, data, target):
        """Chunks of a single variable, filling time then vertical dimensions up to target bytes."""
        # classify the dimensions with smmregrid: vertical dims are the masked (e.g. depth) and other dims
        gridtype = GridType(dims=data.dims)
        # bounds and other auxiliary variables have no time or no horizontal dimensions
        if not gridtype.time_dims or not gridtype.horizontal_dims:
            return {}
        horizontal = [dim for dim in data.dims if dim in gridtype.horizontal_dims]
        time = [dim for dim in data.dims if dim in gridtype.time_dims]
        vertical = [dim for dim in data.dims if dim == gridtype.mask_dim or dim in gridtype.other_dims]

        # horizontal dims are never split, to stay aligned with the on-disk chunks (one horizontal slice)
        chunks = dict.fromkeys(horizontal, -1)
        slice_bytes = data.dtype.itemsize * math.prod(data.sizes[dim] for dim in horizontal)

        # number of horizontal slices fitting in a chunk
        nslices = target // slice_bytes
        if nslices < 1:
            self.logger.warning(
                "Horizontal slice of %s (%d bytes) exceeds the chunk target (%d bytes)", data.name, slice_bytes, target
            )

        for dim in time + vertical:
            size = min(data.sizes[dim], max(nslices, 1))
            chunks[dim] = size
            nslices //= size

        return chunks

    @staticmethod
    def _merge_chunks(chunks, new):
        """Merge new chunks into chunks in place, keeping the smallest size (-1 means whole dimension)."""
        for dim, size in new.items():
            old = chunks.get(dim, -1)
            chunks[dim] = size if old == -1 else (old if size == -1 else min(old, size))

    def create_entry_details(
        self,
        basedir=None,
        catblock=None,
        output_format="netcdf",
        source_grid_name=DEFAULT_DROP_GRID,
        chunk_target=DEFAULT_DROP_CHUNK_TARGET,
    ):
        """
        Create an entry in the catalog for DROP

        Args:
            basedir (str): Base directory for the output files.
            catblock (dict, optional): Existing catalog block to update. Defaults to None if not existing.
            output_format (str): Output format for the catalog entry. Defaults to 'netcdf', alternative is 'zarr'.
            source_grid_name (str): Name of the source grid. Defaults to 'lon-lat'.
                Can be AQUA grid, or 'False' if not applicable.
            chunk_target (str or int): Target chunk size used to define the chunks of a new entry
                (e.g. '128MiB', '0.2GiB'), or 'auto' for Dask auto chunking. Chunks of an existing entry are not modified.
                Defaults to DEFAULT_DROP_CHUNK_TARGET.

        Returns:
            dict: The catalog block with the updated urlpath and metadata.
        """

        file_extension = {"netcdf": ".nc", "zarr": ".zarr"}.get(output_format, output_format)
        urlpath = self.opt.build_path(basedir=basedir, var="*", year="*", output_format=file_extension)
        self.logger.info("Fully expanded urlpath %s", urlpath)
        expanded_urlpath = urlpath

        urlpath = replace_intake_vars(catalog=self.catalog, path=urlpath)
        self.logger.info("New urlpath with intake variables is %s", urlpath)

        if catblock is None:
            # define optimal chunks for DROP outputs, only for new entries
            chunks = self.define_optimal_chunks(expanded_urlpath, output_format=output_format, chunk_target=chunk_target)

            # if the entry is not there, define the block to be uploaded into the catalog
            catblock = {
                "driver": output_format,
                "description": f"AQUA {output_format} DROP-generated data {self.frequency} at {self.resolution}",
                "args": {
                    "urlpath": urlpath,
                    "chunks": chunks,
                },
                "metadata": {
                    "source_grid_name": source_grid_name,
                },
            }
        else:
            # if the entry is there, we just update the urlpath
            # catblock['args']['urlpath'] = self.update_urlpath(catblock['args']['urlpath'], urlpath)
            catblock["args"]["urlpath"] = urlpath
            self.logger.info("Updated urlpath in existing catalog entry to %s", catblock["args"]["urlpath"])

        if output_format == "netcdf":
            #    self.logger.warning("Setting xarray_kwargs for NetCDF driver")
            catblock["args"]["xarray_kwargs"] = {"decode_times": True, "combine": "by_coords"}

        # TODO: check if we need specific xarray_kwargs for zarr, e.g. to support multi-zarr annual files (mirroring NetCDF)
        # elif output_format == "zarr":
        #    # Support multi-zarr annual files (mirroring NetCDF)
        # catblock["args"]["xarray_kwargs"] = {"engine": "zarr", "combine": "by_coords"}
        #    self.logger.warning("Setting xarray_kwargs for Zarr driver")

        # Jinja parameters to be replaced in the urlpath
        jinja_params = {"realization": self.realization, "region": self.region, "stat": self.stat}

        # Apply replacements
        for param_name, param_value in jinja_params.items():
            catblock = replace_urlpath_jinja(catblock, param_value, param_name)
            self.logger.debug("Urlpath after replacing %s: %s", param_name, catblock["args"]["urlpath"])

        return catblock

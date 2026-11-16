"""
Zarr Writer for DROP

Handles atomic writing of climate data to Zarr stores with validation
and metadata consolidation. Optimized for large time-series datasets.

Requires Zarr v3+.
"""

import os
import shutil

import xarray as xr
import zarr
from dask.utils import parse_bytes

from aqua.core.default import DEFAULT_DROP_CHUNK_TARGET
from aqua.core.drop.drop_util import target_chunks, zarr_encoding
from aqua.core.drop.drop_writer_base import BaseWriter
from aqua.core.util.io_util import file_is_complete


class ZarrWriter(BaseWriter):
    """
    Writer for Zarr format in DROP processing.

    Uses atomic two-phase writes and consolidates metadata for efficient reads.
    On-disk chunks are grouped in zarr v3 shards to limit the number of files: each shard
    matches a read chunk of the DROP catalog entry, so that a read task opens a single file.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        compact="xarray",
        chunk_target=DEFAULT_DROP_CHUNK_TARGET,
        **kwargs,
    ):
        """
        Initialize Zarr writer.

        Args:
            tmpdir: Temporary directory for atomic writes
            outdir: Output directory for zarr stores
            compact: Whether to concatenate monthly stores into yearly stores
            chunk_target: Target size of the read chunks and of the shards, as a Dask size string.
                'auto' uses DEFAULT_DROP_CHUNK_TARGET for the shards. Defaults to DEFAULT_DROP_CHUNK_TARGET.
            **kwargs: Additional arguments passed to BaseWriter

        Note:
            Metadata consolidation is always enabled on yearly archives for optimal read performance.
        """
        super().__init__(tmpdir, outdir, **kwargs)
        self.compact = compact
        self.shard_target = parse_bytes(DEFAULT_DROP_CHUNK_TARGET if chunk_target == "auto" else chunk_target)

    def get_extension(self):
        """Return file extension for this format."""
        return ".zarr"

    def validate(self, store_path, min_months=None):
        """
        Validate Zarr store integrity.

        Args:
            store_path: Path to zarr store
            min_months: Minimum number of distinct calendar months the store must span (optional)

        Returns:
            bool: True if valid, False otherwise
        """
        return file_is_complete(store_path, loglevel=self.logger.level, min_months=min_months)

    def _disk_shards(self, data, chunks):
        """
        Shard sizes grouping the on-disk chunks: the read chunks of the catalog entry (see target_chunks).

        Args:
            data: xarray DataArray
            chunks: On-disk chunks, as returned by _disk_chunks

        Returns:
            tuple: Shard size along each dimension, or None if chunks is None or the variable has no time.
        """
        shards = target_chunks(data, self.shard_target) if chunks else {}
        if not shards:
            return None
        return tuple(size if shards.get(dim, -1) == -1 else shards[dim] for dim, size in data.sizes.items())

    def _get_encoding(self, data, var=None):
        """
        Setup encoding with chunks, shards and compression.

        Zarr v3+ only - uses default xarray compression handling.

        Args:
            data: xarray Dataset
            var: Variable name (unused, kept for interface compatibility)

        Returns:
            dict: Encoding configuration or None
        """
        chunks = {var: self._disk_chunks(data[var]) for var in data.data_vars}
        shards = {var: self._disk_shards(data[var], chunks[var]) for var in data.data_vars}
        return zarr_encoding(chunks, shards=shards)

    def _write_chunk_to_disk(self, data, tmpfile, encoding):
        """
        Write data chunk to Zarr store atomically.

        Args:
            data: xarray Dataset/DataArray (already computed)
            tmpfile: Path to temporary zarr store
            encoding: Encoding configuration

        Returns:
            bool: True if write successful
        """
        tmp_path = tmpfile + ".tmp"

        try:
            # Remove any existing tmp
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)

            # Write to temp location
            data.to_zarr(tmp_path, mode="w", consolidated=False, encoding=encoding)

            # Validate temp
            if not self.validate(tmp_path):
                raise ValueError("Validation failed")

            # Atomic move
            if os.path.exists(tmpfile):
                shutil.rmtree(tmpfile)
            shutil.move(tmp_path, tmpfile)

            return True

        except Exception as e:
            self.logger.error("Zarr write failed: %s", e)
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            return False

    def _should_concat(self):
        """Check if yearly Zarr concatenation is enabled."""
        return self.compact is not None

    def _open_files(self, filepaths):
        """Open one or more Zarr stores."""
        return xr.open_mfdataset(filepaths, engine="zarr", combine="by_coords", consolidated=False)

    def concat_year_files(self, var, year, level=None):
        """
        Concatenate monthly zarr stores into a single yearly store.

        Args:
            var: Variable name
            year: Year to concatenate
            level: Level (optional, for filename generation)

        Returns:
            bool or None: True if concatenated, False on error, None if there are not enough monthly
            files/stores to concatenate (nothing is done, it is not a failure)
        """
        if not self._should_concat():
            return False

        # Prepare monthly stores for concatenation (Zarr requires 12 like NetCDF for consistency)
        tmp_monthly_files, year_file, tmp_year_file = self._prepare_concat_monthly_files(
            var, year, level=level, minimum_required=12
        )

        if tmp_monthly_files is None:
            return None

        try:
            # Open all monthly stores using class method
            ds = self._open_files(tmp_monthly_files)

            # Write to yearly store in tmpdir, with one dask chunk per shard (shards are written whole)
            encoding = self._get_encoding(ds)
            for name, enc in encoding.items():
                if "shards" in enc:
                    ds[name] = ds[name].chunk(dict(zip(ds[name].dims, enc["shards"])))
            ds.to_zarr(tmp_year_file, mode="w", consolidated=False, encoding=encoding)

            # Always consolidate metadata for optimal read performance
            self.consolidate_metadata(tmp_year_file)
            self.logger.info("Consolidated metadata for %s", os.path.basename(tmp_year_file))

            # Move yearly store to output and cleanup monthly stores
            shutil.move(tmp_year_file, year_file)
            self._cleanup_monthly_files(tmp_monthly_files)

            return True

        except Exception as e:
            self.logger.error("Failed to concatenate monthly stores: %s", e)
            return False

    def consolidate_metadata(self, store_path):
        """
        Consolidate zarr metadata for faster reads.

        Args:
            store_path: Path to zarr store

        Returns:
            bool: True if successful
        """
        try:
            zarr.consolidate_metadata(store_path)
            self.logger.info("Metadata consolidated: %s", store_path)
            return True
        except Exception as e:
            self.logger.error("Consolidation failed for %s: %s", store_path, e)
            return False

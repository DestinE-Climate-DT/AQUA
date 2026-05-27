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

from aqua.core.drop.drop_writer_base import BaseWriter
from aqua.core.util.io_util import file_is_complete

# zarr chunking defaults
ZARR_CHUNKS = {"time": 1, "lat": None, "lon": None}


class ZarrWriter(BaseWriter):
    """
    Writer for Zarr format in DROP processing.

    Uses atomic two-phase writes and consolidates metadata for efficient reads.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        **kwargs,
    ):
        """
        Initialize Zarr writer.

        Args:
            tmpdir: Temporary directory for atomic writes
            outdir: Output directory for zarr stores
            **kwargs: Additional arguments passed to BaseWriter

        Note:
            Metadata consolidation is always enabled on yearly archives for optimal read performance.
        """
        super().__init__(tmpdir, outdir, **kwargs)
        self.chunks = ZARR_CHUNKS

    def get_extension(self):
        """Return file extension for this format."""
        return ".zarr"

    def validate(self, store_path):
        """
        Validate Zarr store integrity.

        Args:
            store_path: Path to zarr store

        Returns:
            bool: True if valid, False otherwise
        """
        return file_is_complete(store_path, loglevel=self.logger.level)

    def _get_encoding(self, data, var=None):
        """
        Setup encoding with chunks and compression.

        Zarr v3+ only - uses default xarray compression handling.

        Args:
            data: xarray Dataset
            var: Variable name (unused, kept for interface compatibility)

        Returns:
            dict: Encoding configuration or None
        """
        if not self.chunks:
            return None
        self.logger.debug("Using xarray default compression for zarr v3")
        return self._build_zarr_encoding(data, time_chunk=self.chunks.get("time", 1))

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
        """Zarr always concatenates monthly stores into yearly."""
        return True

    def _open_files(self, filepaths):
        """Open one or more Zarr stores."""
        return xr.open_mfdataset(filepaths, engine="zarr", combine="by_coords", consolidated=False)

    def concat_year_files(self, var, year):
        """
        Concatenate monthly zarr stores into a single yearly store.

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        # Prepare monthly stores for concatenation (Zarr requires 12 like NetCDF for consistency)
        tmp_monthly_files, year_file, tmp_year_file = self._prepare_concat_monthly_files(var, year, minimum_required=12)

        if tmp_monthly_files is None:
            return False

        try:
            # Open all monthly stores using class method
            ds = self._open_files(tmp_monthly_files)

            # Write to yearly store in tmpdir
            encoding = self._get_encoding(ds)
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

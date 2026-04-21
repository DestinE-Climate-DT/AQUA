"""
Zarr Writer for DROP

Handles atomic writing of climate data to Zarr stores with validation
and metadata consolidation. Optimized for large time-series datasets.

Requires Zarr v3+.
"""

import glob
import os
import shutil
from time import time

import numpy as np
import pandas as pd
import xarray as xr
import zarr
from dask.diagnostics import ProgressBar
from dask.distributed import progress
from dask.distributed.diagnostics import MemorySampler

from aqua.core.drop.drop_util import move_tmp_files
from aqua.core.logger import log_configure


class ZarrWriter:
    """
    Writer for Zarr format in DROP processing.

    Uses atomic two-phase writes and consolidates metadata for efficient reads.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        chunks=None,
        compressor="auto",
        dask_client=None,
        performance_reporting=False,
        loglevel="WARNING",
    ):
        """
        Initialize Zarr writer.

        Args:
            tmpdir: Temporary directory for atomic writes
            outdir: Output directory for zarr stores
            chunks: Dict of chunk sizes (e.g. {'time': 1, 'lat': None, 'lon': None})
            compressor: 'auto' for Blosc/zstd or custom numcodecs compressor
            dask_client: Dask client for distributed computing
            performance_reporting: Enable performance reporting
            loglevel: Logging level

        Note:
            Metadata consolidation is always enabled on yearly archives for optimal read performance.
        """
        self.tmpdir = tmpdir
        self.outdir = outdir
        self.chunks = chunks or {"time": 1, "lat": None, "lon": None}
        self.compressor = compressor
        self.dask_client = dask_client
        self.performance_reporting = performance_reporting
        self.logger = log_configure(loglevel, "ZarrWriter")
        self.stores_written = set()  # Track stores for finalization

    def get_extension(self):
        """Return file extension for this format."""
        return ".zarr"

    def validate(self, store_path):
        """
        Validate Zarr store integrity.

        Args:
            store_path: Path to zarr store

        Returns:
            tuple: (bool, result) - success flag and either Dataset or error message
        """
        try:
            ds = xr.open_zarr(store_path, consolidated=False)
            if "time" not in ds.dims:
                return False, "No time dimension"
            if len(ds.time) == 0:
                return False, "Empty time"
            # Check for duplicates
            if len(ds.time) != len(set(ds.time.values)):
                return False, "Duplicate times"
            # Check sorted
            if len(ds.time) > 1 and not (ds.time.diff("time") > np.timedelta64(0, "ns")).all():
                return False, "Time not sorted"
            return True, ds
        except Exception as e:
            return False, str(e)

    def _should_append(self, ds_existing, ds_new):
        """
        Check if new data can be safely appended.

        Args:
            ds_existing: Existing dataset in store
            ds_new: New dataset to append

        Returns:
            bool: True if safe to append
        """
        try:
            existing_times = set(ds_existing.time.values)
            new_times = ds_new.time.values

            if any(t in existing_times for t in new_times):
                self.logger.warning("Time overlap detected, skipping append")
                return False
            return True
        except Exception as e:
            self.logger.warning("Cannot check overlap: %s", e)
            return False

    def _setup_encoding(self, data):
        """
        Setup encoding with chunks and compression.

        Zarr v3+ only - uses default xarray compression handling.

        Args:
            data: xarray Dataset

        Returns:
            dict: Encoding configuration
        """
        encoding = {}

        # Get variable list
        vars_to_encode = list(data.data_vars)

        # Setup chunking
        if self.chunks:
            for var in vars_to_encode:
                # Convert None to actual dimension size
                chunks_resolved = tuple(
                    self.chunks.get(dim, data[var].sizes[dim]) if self.chunks.get(dim) is not None else data[var].sizes[dim]
                    for dim in data[var].dims
                )
                encoding[var] = {"chunks": chunks_resolved}

        # Compression: let xarray handle zarr v3 codecs by default
        # zarr v3 uses a different API (codecs instead of compressor)
        # xarray will apply default compression if not specified
        self.logger.debug("Using xarray default compression for zarr v3")

        return encoding or None

    def write_monthly_chunk(self, data, var, year, month):
        """
        Write a single monthly zarr store (mirroring NetCDF monthly files).

        Args:
            data: xarray Dataset/DataArray for one month
            var: Variable name
            year: Year
            month: Month

        Returns:
            bool: True if write successful
        """
        # Monthly store naming
        monthly_store_name = f"{var}_{year}{month:02d}_monthly.zarr"
        monthly_store_path = os.path.join(self.tmpdir, monthly_store_name)

        # Check if monthly store already exists (recovery scenario)
        if os.path.exists(monthly_store_path):
            valid, _ = self.validate(monthly_store_path)
            if valid:
                self.logger.info("Monthly store %s already exists and is valid, skipping...", monthly_store_name)
                return True
            else:
                self.logger.warning("Invalid monthly store %s, recreating...", monthly_store_name)
                shutil.rmtree(monthly_store_path)

        # Convert DataArray to Dataset if needed
        if isinstance(data, xr.DataArray):
            var_name = data.name or "data"
            data = data.to_dataset(name=var_name)

        # Compute data with performance monitoring
        if self.dask_client is not None:
            self.logger.info("Computing data with Dask monitoring...")
            if self.performance_reporting:
                job = data.persist()
                progress(job)
                data = job.compute()
            else:
                ms = MemorySampler()
                with ms.sample("chunk"):
                    job = data.persist()
                    progress(job)
                    data = job.compute()
                array_data = np.array(ms.samples["chunk"])
                avg_mem = np.mean(array_data[:, 1]) / 1e9
                max_mem = np.max(array_data[:, 1]) / 1e9
                self.logger.info("Avg memory used: %.2f GiB, Peak memory used: %.2f GiB", avg_mem, max_mem)
        else:
            if hasattr(data, "compute"):
                with ProgressBar():
                    data = data.compute()

        # Write monthly store atomically
        tmp_path = monthly_store_path + ".tmp"
        encoding = self._setup_encoding(data)

        try:
            data.to_zarr(tmp_path, mode="w", consolidated=False, encoding=encoding)

            # Validate temp
            valid, result = self.validate(tmp_path)
            if not valid:
                raise ValueError(f"Validation failed: {result}")

            # Atomic move
            shutil.move(tmp_path, monthly_store_path)
            self.logger.info("Successfully wrote monthly zarr store: %s", monthly_store_name)
            return True

        except Exception as e:
            self.logger.error("Monthly zarr write failed: %s", e)
            if os.path.exists(tmp_path):
                shutil.rmtree(tmp_path)
            return False

    def append_chunk(self, data, store_path):
        """
        Legacy method - kept for backward compatibility.
        Delegates to write_monthly_chunk with extracted year/month.

        Args:
            data: xarray Dataset/DataArray to append
            store_path: Path to final zarr store location (in outdir)

        Returns:
            bool: True if write successful
        """
        # Convert DataArray to Dataset if needed
        if isinstance(data, xr.DataArray):
            var_name = data.name or "data"
            data = data.to_dataset(name=var_name)

        # Compute data with performance monitoring (if dask)
        if self.dask_client is not None:
            self.logger.info("Computing data with Dask monitoring...")
            if self.performance_reporting:
                # Full Dask dashboard support
                job = data.persist()
                progress(job)
                data = job.compute()
            else:
                # Memory monitoring
                ms = MemorySampler()
                with ms.sample("chunk"):
                    job = data.persist()
                    progress(job)
                    data = job.compute()
                array_data = np.array(ms.samples["chunk"])
                avg_mem = np.mean(array_data[:, 1]) / 1e9
                max_mem = np.max(array_data[:, 1]) / 1e9
                self.logger.info("Avg memory used: %.2f GiB, Peak memory used: %.2f GiB", avg_mem, max_mem)
        else:
            # Local computation with progress bar
            if hasattr(data, "compute"):
                with ProgressBar():
                    data = data.compute()

        # Extract year/month from data for monthly pattern
        if hasattr(data, "time") and len(data.time) > 0:
            year = int(data.time.dt.year.values[0])
            month = int(data.time.dt.month.values[0])
            var = list(data.data_vars)[0] if isinstance(data, xr.Dataset) else data.name
            return self.write_monthly_chunk(data, var, year, month)
        else:
            self.logger.error("Cannot determine year/month from data")
            return False

    def concat_year_stores(self, var, year):
        """
        Concatenate monthly zarr stores into a single yearly store (mirroring NetCDF).

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        # Find monthly stores for this year
        monthly_pattern = os.path.join(self.tmpdir, f"{var}_{year}??_monthly.zarr")
        monthly_stores = sorted(glob.glob(monthly_pattern))

        if len(monthly_stores) == 0:
            self.logger.warning("No monthly stores found for %s year %s", var, year)
            return False

        self.logger.info("Merging %d monthly zarr stores for %s, year %s...", len(monthly_stores), var, year)

        try:
            # Open all monthly stores and concatenate
            ds = xr.open_mfdataset(monthly_stores, engine="zarr", combine="by_coords", consolidated=False)

            # Write to yearly store in tmpdir
            year_store_name = f"{var}_{year}.zarr"
            year_store_path = os.path.join(self.tmpdir, year_store_name)

            if os.path.exists(year_store_path):
                shutil.rmtree(year_store_path)

            encoding = self._setup_encoding(ds)
            ds.to_zarr(year_store_path, mode="w", consolidated=False, encoding=encoding)

            # Always consolidate metadata on yearly store for optimal read performance
            self.consolidate_metadata(year_store_path)
            self.logger.info("Consolidated metadata for yearly store %s", year_store_name)

            # Cleanup monthly stores
            for monthly_store in monthly_stores:
                self.logger.info("Cleaning monthly store %s...", os.path.basename(monthly_store))
                shutil.rmtree(monthly_store)

            # Track for final move
            self.stores_written.add(self.get_filename(var, year=year))
            return True

        except Exception as e:
            self.logger.error("Failed to concatenate year stores: %s", e)
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

    def get_filename(self, var, year=None, month=None, tmp=False):
        """
        Generate Zarr store path (yearly stores like NetCDF).

        Args:
            var: Variable name
            year: Year for yearly stores
            month: Month (ignored - zarr appends to yearly store)
            tmp: If True, return path in tmpdir

        Returns:
            str: Full path to zarr store
        """
        # Multi-zarr annual pattern (mirroring NetCDF)
        if year is not None:
            store_name = f"{var}_{year}.zarr"
        else:
            store_name = f"{var}.zarr"

        if tmp:
            return os.path.join(self.tmpdir, store_name)
        return os.path.join(self.outdir, store_name)

    def check_integrity(self, var, overwrite=False):
        """
        Check integrity of Zarr stores for a variable (yearly stores).

        Args:
            var: Variable name
            overwrite: If True, always report incomplete

        Returns:
            dict: {
                'complete': bool,
                'last_record': str (YYYYMMDD format) or None,
                'message': str
            }
        """
        if overwrite:
            return {"complete": False, "last_record": None, "message": "Overwrite mode enabled"}

        # Check for yearly stores (multi-zarr pattern)
        year_stores = glob.glob(self.get_filename(var, year="*"))

        if not year_stores:
            return {"complete": False, "last_record": None, "message": "No stores found"}

        # Validate all stores
        checks = []
        for store in year_stores:
            valid, _ = self.validate(store)
            checks.append(valid)

        all_checks_true = all(checks)

        if all_checks_true:
            try:
                # Open all stores to find last record
                if len(year_stores) == 1:
                    ds = xr.open_zarr(year_stores[0], consolidated=False)
                else:
                    ds = xr.open_mfdataset(year_stores, engine="zarr", combine="by_coords")
                last_record = ds.time[-1].values
                last_record_str = pd.to_datetime(last_record).strftime("%Y%m%d")
                return {"complete": True, "last_record": last_record_str, "message": f"All {len(year_stores)} stores complete"}
            except Exception as e:
                return {"complete": False, "last_record": None, "message": f"Error reading stores: {e}"}
        else:
            return {"complete": False, "last_record": None, "message": f"{sum(checks)}/{len(checks)} stores complete"}

    def write_variable(self, data, var, overwrite=False, definitive=True, performance_reporting=False, history_callback=None):
        """
        Write complete variable to yearly Zarr stores (mirroring NetCDF).

        Creates one .zarr store per year, each built month-by-month in tmpdir.
        Consolidates metadata at the end of each year.

        Args:
            data: xarray DataArray with processed data
            var: Variable name
            overwrite: Overwrite existing stores
            definitive: Actually write files (vs dry-run)
            performance_reporting: Limit to first month only
            history_callback: Optional function to append history metadata

        Returns:
            bool: True if successful
        """
        # Split data into years
        years = sorted(set(data.time.dt.year.values))
        if performance_reporting:
            years = [years[0]]

        for year in years:
            self.logger.info("Processing year %s...", str(year))

            # Yearly store path (final destination in outdir)
            year_store_path = self.get_filename(var, year=year)

            # Check if yearly store exists
            if os.path.exists(year_store_path):
                valid, result = self.validate(year_store_path)
                if valid and not overwrite:
                    self.logger.info("Yearly store %s already exists and is valid, skipping...", year_store_path)
                    continue
                if overwrite:
                    self.logger.warning("Yearly store %s exists, will overwrite...", year_store_path)
                    shutil.rmtree(year_store_path)

            year_data = data.sel(time=data.time.dt.year == year)

            # Process month by month, appending to same yearly store in tmpdir
            months = sorted(set(year_data.time.dt.month.values))
            if performance_reporting:
                months = [months[0]]

            for month in months:
                self.logger.info("Processing month %s...", str(month))
                month_data = year_data.sel(time=year_data.time.dt.month == month)

                # Apply history if callback provided
                if history_callback:
                    month_data = history_callback(month_data)

                # Write monthly zarr store (recovery-safe)
                if definitive:
                    t_start = time()
                    success = self.write_monthly_chunk(month_data, var, year, month)
                    t_elapsed = time() - t_start
                    if success:
                        self.logger.info("Chunk execution time: %.2f", t_elapsed)
                    else:
                        self.logger.error("Failed to write chunk for %s-%s", year, month)

                del month_data

            del year_data

            # Concatenate monthly stores into yearly store
            if definitive:
                self.logger.info("Concatenating monthly stores for year %s...", year)
                concat_success = self.concat_year_stores(var, year)
                if not concat_success:
                    self.logger.error("Failed to create yearly store for %s", year)

        # Move all yearly stores from tmpdir to outdir
        if definitive:
            self.logger.info("Moving yearly stores from tmpdir to outdir...")
            move_tmp_files(self.tmpdir, self.outdir)

        return True

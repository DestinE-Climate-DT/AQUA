"""
Base Writer for DROP

Abstract base class implementing the Template Method pattern for
NetCDF and Zarr writers. Contains common logic for writing climate data chunks.
"""

import glob
import os
import shutil
from abc import ABC, abstractmethod
from datetime import datetime
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

xr.set_options(keep_attrs=True)


class BaseWriter(ABC):
    """
    Abstract base writer for DROP processing.

    Implements common logic for monthly file/store creation and yearly concatenation.
    Subclasses must implement format-specific methods.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        filename_builder=None,
        loglevel="WARNING",
    ):
        """
        Initialize base writer.

        Args:
            tmpdir: Temporary directory for intermediate files
            outdir: Output directory for final files
            filename_builder: OutputPathBuilder instance for filename generation
            loglevel: Logging level
            _chunk_stats: the list of chunk stats dictionaries, each with keys:
                var, year, month, elapsed, mem, size_bytes, throughput_mib_s
            _last_mem_stats: the last memory stats dictionary with keys avg_mem and max_mem, or None
            _last_chunk_size_bytes: the size in bytes of the last chunk, or None
        """
        self.tmpdir = tmpdir
        self.outdir = outdir
        self.filename_builder = filename_builder
        self.logger = log_configure(loglevel, self.__class__.__name__)
        self._chunk_stats = []
        self._last_mem_stats = None
        self._last_chunk_size_bytes = None

    @abstractmethod
    def get_extension(self):
        """
        Return file extension for this format.

        Returns:
            str: File extension (e.g., '.nc', '.zarr')
        """
        pass

    @abstractmethod
    def validate(self, path):
        """
        Validate file/store integrity.

        Args:
            path: Path to file/store to validate

        Returns:
            bool: True if valid, False otherwise
        """
        pass

    @abstractmethod
    def _get_encoding(self, data, var=None):
        """
        Get format-specific encoding configuration.

        Args:
            data: xarray Dataset/DataArray
            var: Variable name (optional, for per-variable encoding)

        Returns:
            dict: Encoding configuration or None
        """
        pass

    @abstractmethod
    def _write_chunk_to_disk(self, data, tmpfile, encoding):
        """
        Write data chunk to disk using format-specific method.

        Args:
            data: xarray Dataset/DataArray (already computed)
            tmpfile: Path to temporary file/store
            encoding: Encoding configuration

        Returns:
            bool: True if write successful
        """
        pass

    @abstractmethod
    def _should_concat(self):
        """
        Check if concatenation should be performed.

        Returns:
            bool: True if concatenation is enabled
        """
        pass

    @abstractmethod
    def _open_files(self, filepaths):
        """
        Open one or more files/stores.

        xarray.open_mfdataset handles both single and multiple files,
        so no need for separate methods.

        Args:
            filepaths: Path or list of paths to files/stores

        Returns:
            xarray.Dataset: Opened dataset
        """
        pass

    def _build_zarr_encoding(self, data, time_chunk, compressor_level=1):
        """
        Build per-variable chunk encoding with explicit time chunking and full spatial dims.

        Shared by ZarrWriter and IcechunkWriter. For each variable in ``data``,
        constructs a chunk tuple where the ``time`` dimension uses ``time_chunk``
        and all other dimensions use the full dimension size (no chunking).

        Args:
            data: xarray Dataset
            time_chunk (int): Number of time steps per chunk.
            compressor_level (int): Compression level for GzipCodec (1-9)

        Returns:
            dict: Encoding configuration or None if data has no data_vars.
        """
        encoding = {}
        compressor = zarr.codecs.GzipCodec(level=compressor_level)
        for var_name in data.data_vars:
            chunks = tuple(time_chunk if dim == "time" else data[var_name].sizes[dim] for dim in data[var_name].dims)
            encoding[var_name] = {"chunks": chunks, "compressor": compressor}
        return encoding or None

    def _compute_data(self, data, dask=False, performance_reporting=False):
        """
        Compute data with Dask monitoring and performance reporting.

        Args:
            data: xarray Dataset/DataArray (possibly lazy/dask)
            dask: If True, use Dask for distributed computing
            performance_reporting: Enable performance reporting

        Returns:
            xarray Dataset/DataArray: Computed data
        """
        if dask:
            self.logger.info("Computing data with Dask monitoring...")
            if performance_reporting:
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
                self.logger.info(
                    "Avg memory used: %.2f GiB, Peak memory used: %.2f GiB",
                    avg_mem,
                    max_mem,
                )
                self._last_mem_stats = {"avg_mem": avg_mem, "max_mem": max_mem}
        else:
            if hasattr(data, "compute"):
                with ProgressBar():
                    data = data.compute()
        return data

    def _to_dataset(self, data, var):
        """Convert DataArray to Dataset, preserving Dataset-level attributes.

        Args:
            data: xarray Dataset or DataArray.
            var: Variable name used as fallback when the DataArray has no name.

        Returns:
            xarray.Dataset: Dataset with original attributes intact.
        """
        if isinstance(data, xr.DataArray):
            attrs = data.attrs.copy()
            data = data.to_dataset(name=data.name or var or "data")
            data.attrs.update(attrs)
        return data

    def _iter_years(self, data, performance_reporting):
        """Yield (year, year_data) pairs with Dataset-level attrs preserved after slicing.

        Args:
            data: xarray Dataset with a time dimension.
            performance_reporting: If True, yield only the first year.

        Yields:
            tuple: (year, year_data) where year_data has original attrs restored.
        """
        years = sorted(set(data.time.dt.year.values))
        if performance_reporting:
            years = [years[0]]
        for year in years:
            yield year, data.sel(time=data.time.dt.year == year)

    def _iter_months_in_year(self, year_data, performance_reporting):
        """Yield (month, month_data) pairs with Dataset-level attrs preserved after slicing.

        Args:
            year_data: xarray Dataset for a single year (attrs already restored by _iter_years).
            performance_reporting: If True, yield only the first month.

        Yields:
            tuple: (month, month_data) where month_data has original attrs restored.
        """
        months = sorted(set(year_data.time.dt.month.values))
        if performance_reporting:
            months = [months[0]]
        for month in months:
            yield month, year_data.sel(time=year_data.time.dt.month == month)

    def _write_chunk(self, data, var, year, month, dask=False, performance_reporting=False):
        """
        Write a single monthly chunk.

        Template method that orchestrates the write process:
        1. Compute data with monitoring
        2. Get encoding
        3. Write to disk using format-specific method

        Args:
            data: xarray Dataset/DataArray for one month
            var: Variable name
            year: Year
            month: Month
            dask: If True, use Dask for distributed computing
            performance_reporting: Enable performance reporting

        Returns:
            bool: True if write successful
        """
        tmpfile = self.get_filename(var, year=year, month=month, tmp=True)

        # Remove existing tmpfile
        if os.path.exists(tmpfile):
            if os.path.isdir(tmpfile):
                shutil.rmtree(tmpfile)
            else:
                os.remove(tmpfile)
            self.logger.warning("Removed existing tmpfile %s", tmpfile)

        self.logger.info("Computing to write file %s...", tmpfile)

        # Convert DataArray to Dataset if needed, preserving attributes
        data = self._to_dataset(data, var)

        # Compute data
        data = self._compute_data(data, dask=dask, performance_reporting=performance_reporting)
        self._last_chunk_size_bytes = data.nbytes

        # Get encoding
        encoding = self._get_encoding(data, var)

        # Write to disk
        success = self._write_chunk_to_disk(data, tmpfile, encoding)

        if success:
            self.logger.info("Writing file %s successful!", tmpfile)

        return success

    def get_filename(self, var, year=None, month=None, tmp=False):
        """
        Generate filename/storename (monthly or yearly).

        Args:
            var: Variable name
            year: Year (for yearly or monthly files)
            month: Month (for monthly files, optional)
            tmp: If True, return path in tmpdir

        Returns:
            str: Full path to file/store
        """
        ext = self.get_extension()

        filename = self.filename_builder.build_filename(var=var, year=year, month=month)
        # Replace extension if needed
        if not filename.endswith(ext):
            filename = os.path.splitext(filename)[0] + ext

        if tmp:
            return os.path.join(self.tmpdir, os.path.basename(filename))
        return os.path.join(self.outdir, filename) if not os.path.isabs(filename) else filename

    def _get_and_validate_monthly_files(self, var, year, minimum_required):
        """
        Get and validate monthly files/stores for concatenation.

        Args:
            var: Variable name
            year: Year
            minimum_required: Minimum number of files required

        Returns:
            tuple: (monthly_files: list, is_valid: bool)
        """
        monthly_pattern = self.get_filename(var, year, month="??")
        monthly_files = sorted(glob.glob(monthly_pattern))
        count = len(monthly_files)

        if count == 0:
            self.logger.warning("No monthly files found for %s year %s", var, year)
            return [], False

        if count < minimum_required:
            self.logger.warning(
                "Found only %d monthly files for %s year %s, expected at least %d",
                count,
                var,
                year,
                minimum_required,
            )
            return monthly_files, False

        self.logger.debug("Found %d monthly files for %s year %s", count, var, year)
        return monthly_files, True

    def _cleanup_monthly_files(self, monthly_files):
        """
        Remove monthly files/stores after successful concatenation.

        Args:
            monthly_files: List of paths to remove
        """
        for monthly_file in monthly_files:
            basename = os.path.basename(monthly_file)
            if os.path.isdir(monthly_file):
                self.logger.info("Cleaning monthly store %s...", basename)
                shutil.rmtree(monthly_file)
            else:
                self.logger.info("Cleaning monthly file %s...", basename)
                os.remove(monthly_file)

    def _prepare_concat_monthly_files(self, var, year, minimum_required):
        """
        Prepare monthly files for concatenation into yearly file.

        Common logic for both NetCDF and Zarr:
        1. Get and validate monthly files
        2. Move them to tmpdir for safety
        3. Clean any existing yearly files
        4. Return paths for concatenation

        Args:
            var: Variable name
            year: Year to concatenate
            minimum_required: Minimum number of monthly files required

        Returns:
            tuple: (tmp_monthly_files, year_file, tmp_year_file) or (None, None, None) if invalid
        """
        # Get and validate monthly files
        monthly_files, is_valid = self._get_and_validate_monthly_files(var, year, minimum_required=minimum_required)

        if not is_valid:
            return None, None, None

        self.logger.info("Creating yearly file for %s, year %s from %d monthly files...", var, year, len(monthly_files))

        # Get output paths
        year_file = self.get_filename(var, year)
        tmp_year_file = os.path.join(self.tmpdir, os.path.basename(year_file))

        # Move monthly files to tmpdir for safety
        for monthly_file in monthly_files:
            shutil.move(monthly_file, self.tmpdir)

        # Clean any existing output files
        for f in [tmp_year_file, year_file]:
            if os.path.exists(f):
                if os.path.isdir(f):
                    shutil.rmtree(f)
                else:
                    os.remove(f)

        # Get the moved files in tmpdir - they keep the same basename
        tmp_monthly_files = [os.path.join(self.tmpdir, os.path.basename(f)) for f in monthly_files]

        return tmp_monthly_files, year_file, tmp_year_file

    @abstractmethod
    def concat_year_files(self, var, year):
        """
        Concatenate monthly files into a single yearly file/store.

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        pass

    def check_integrity(self, var, overwrite=False, end_date=None):
        """
        Check integrity of files/stores for a variable.

        Args:
            var: Variable name
            overwrite: If True, always report incomplete
            end_date: Unused in the base implementation; accepted for interface
                compatibility with subclasses (e.g. IcechunkWriter) that need
                to verify coverage up to a specific date.

        Returns:
            dict: {
                'complete': bool,
                'last_record': str (YYYYMMDD format) or None,
                'message': str
            }
        """
        if overwrite:
            return {"complete": False, "last_record": None, "message": "Overwrite mode enabled"}

        # Check yearly files/stores
        yearfiles = self.get_filename(var, year="*")
        yearfiles = glob.glob(yearfiles)

        if not yearfiles:
            return {"complete": False, "last_record": None, "message": "No files found"}

        checks = [self.validate(yearfile) for yearfile in yearfiles]
        all_checks_true = all(checks)

        if all_checks_true:
            try:
                # xr.open_mfdataset works with both single and multiple files
                ds = self._open_files(yearfiles)
                last_record = ds.time[-1].values
                last_record_str = pd.to_datetime(last_record).strftime("%Y%m%d")
                return {
                    "complete": True,
                    "last_record": last_record_str,
                    "message": f"All {len(yearfiles)} files complete",
                }
            except Exception as e:
                return {"complete": False, "last_record": None, "message": f"Error reading files: {e}"}
        else:
            return {
                "complete": False,
                "last_record": None,
                "message": f"{sum(checks)}/{len(checks)} files complete",
            }

    def write_variable(
        self,
        data,
        var,
        overwrite=False,
        definitive=True,
        dask=False,
        performance_reporting=False,
        stats_file=None,
    ):
        """
        Write complete variable with all year/month logic.

        Template method that orchestrates the complete write process:
        1. Split data into years
        2. For each year:
           - Check if yearly file exists
           - Split into months
           - For each month:
             * Check if monthly file exists
             * Write monthly chunk
             * Validate tmpfile
             * Move tmpfile to outdir (IMMEDIATELY)
           - Concatenate monthly files into yearly file
        3. Return success status

        Args:
            data: xarray DataArray with processed data (history already applied)
            var: Variable name
            overwrite: Overwrite existing files
            definitive: Actually write files (vs dry-run)
            dask: If True, use Dask for distributed computing
            performance_reporting: Limit to first month only
            stats_file: Path to stats text file for immediate per-chunk writes. None disables writing.
        """
        for year, year_data in self._iter_years(data, performance_reporting):
            self.logger.info("Processing year %s...", str(year))
            yearfile = self.get_filename(var, year=year)

            # Check if yearly file exists
            if os.path.exists(yearfile):
                if not overwrite:
                    self.logger.info("Yearly file %s already exists, skipping...", yearfile)
                    continue
                self.logger.warning("Yearly file %s already exists, overwriting...", yearfile)

            for month, month_data in self._iter_months_in_year(year_data, performance_reporting):
                self.logger.info("Processing month %s...", str(month))
                monthfile = self.get_filename(var, year=year, month=month)

                # Check if monthly file exists
                if os.path.exists(monthfile):
                    if not overwrite:
                        self.logger.info("Monthly file %s already exists, skipping...", monthfile)
                        continue
                    self.logger.warning("Monthly file %s already exists, overwriting...", monthfile)

                # Write file
                if definitive:
                    tmpfile = self.get_filename(var, year=year, month=month, tmp=True)
                    t_start = time()
                    success = self._write_chunk(
                        month_data, var, year, month, dask=dask, performance_reporting=performance_reporting
                    )
                    t_elapsed = time() - t_start
                    self.logger.info("Chunk execution time: %.2f", t_elapsed)
                    self._record_chunk_stats(var, year, month, t_elapsed, self._last_chunk_size_bytes, stats_file)

                    if not success:
                        self.logger.error("Failed to write chunk for %s-%s", year, month)
                        continue

                    # Validate temp file
                    if not self.validate(tmpfile):
                        self.logger.error("Something has gone wrong in %s!", tmpfile)
                        continue

                    # Move IMMEDIATELY (NetCDF timing, not Zarr's deferred move)
                    self.logger.info("Moving temporary file %s to %s", tmpfile, monthfile)
                    move_tmp_files(self.tmpdir, self.outdir)

            # Concatenate into yearly file if concat enabled
            if definitive and self._should_concat():
                self.concat_year_files(var, year)

        return True

    def _record_chunk_stats(self, var, year, month, t_elapsed, size_bytes, stats_file):
        """Record per-chunk performance stats and optionally append to the stats file.

        Resets ``_last_mem_stats`` and ``_last_chunk_size_bytes`` after recording
        so the next chunk starts with clean state.

        Args:
            var: Variable name.
            year: Year of the chunk.
            month: Month of the chunk.
            t_elapsed: Wall-clock elapsed time in seconds.
            size_bytes: Uncompressed data size in bytes, or None.
            stats_file: Path to the stats text file, or None to skip file write.
        """
        entry = {
            "var": var,
            "year": year,
            "month": month,
            "elapsed": t_elapsed,
            "mem": self._last_mem_stats,
            "size_bytes": size_bytes,
            "throughput_mib_s": (size_bytes / (1024**2)) / t_elapsed if (size_bytes is not None and t_elapsed > 0) else None,
        }
        self._chunk_stats.append(entry)
        self._last_mem_stats = None
        self._last_chunk_size_bytes = None
        if stats_file is not None:
            self._write_chunk_stat_line(entry, stats_file)

    def _write_chunk_stat_line(self, entry, stats_file):
        """Write a single chunk stat line immediately to the stats file."""
        ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mem = entry.get("mem")
        size_bytes = entry.get("size_bytes")
        tp = entry.get("throughput_mib_s")
        mem_str = f"  avg_mem={mem['avg_mem']:.2f} GiB  peak_mem={mem['max_mem']:.2f} GiB" if mem is not None else ""
        size_str = f"  size={size_bytes / (1024**2):.1f} MiB" if size_bytes is not None else ""
        tp_str = f"  throughput={tp:.2f} MiB/s" if tp is not None else ""
        line = (
            f"[{ts}] CHUNK  var={entry['var']}  year={entry['year']}  "
            f"month={entry['month']:02d}  elapsed={entry['elapsed']:.2f}s{size_str}{tp_str}{mem_str}\n"
        )
        with open(stats_file, "a", encoding="utf-8") as fh:
            fh.write(line)

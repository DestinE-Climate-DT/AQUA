"""
Base Writer for DROP

Abstract base class implementing the Template Method pattern for
NetCDF and Zarr writers. Contains common logic for writing climate data chunks.
"""

import glob
import os
import shutil
from abc import ABC, abstractmethod
from time import time

import numpy as np
import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import progress
from dask.distributed.diagnostics import MemorySampler

from aqua.core.drop.drop_util import move_tmp_files
from aqua.core.logger import log_configure


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
        """
        self.tmpdir = tmpdir
        self.outdir = outdir
        self.filename_builder = filename_builder
        self.logger = log_configure(loglevel, self.__class__.__name__)

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
    def _concat_year_files(self, var, year):
        """
        Concatenate monthly files into a single yearly file/store.

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
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
        else:
            if hasattr(data, "compute"):
                with ProgressBar():
                    data = data.compute()
        return data

    def _write_monthly_chunk(self, data, var, year, month, dask=False, performance_reporting=False):
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
        if isinstance(data, xr.DataArray):
            var_name = data.name or var or "data"
            # Preserve DataArray attributes before conversion
            attrs = data.attrs.copy()
            data = data.to_dataset(name=var_name)
            # Apply attributes to Dataset level
            data.attrs.update(attrs)

        # Compute data
        data = self._compute_data(data, dask=dask, performance_reporting=performance_reporting)

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
        else:
            return os.path.join(self.outdir, filename) if not os.path.isabs(filename) else filename

    def _get_and_validate_monthly_files(self, var, year, minimum_required):
        """
        Get and validate monthly files/stores for concatenation.

        Args:
            var: Variable name
            year: Year
            minimum_required: Minimum number of files required

        Returns:
            tuple: (monthly_files: list, is_valid: bool, message: str)
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

    def concat_year_files(self, var, year, get_filename_fn=None):
        """
        Public wrapper for concatenating monthly files into yearly files.

        Args:
            var: Variable name
            year: Year to concatenate
            get_filename_fn: Legacy parameter, ignored (uses self.get_filename)

        Returns:
            bool: True if successful
        """
        return self._concat_year_files(var, year)

    def check_integrity(self, var, overwrite=False):
        """
        Check integrity of files/stores for a variable.

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

        Returns:
            bool: True if successful
        """
        # Preserve original attributes for re-application after slicing
        original_attrs = data.attrs.copy()

        # Split data into years
        years = sorted(set(data.time.dt.year.values))
        if performance_reporting:
            years = [years[0]]

        for year in years:
            self.logger.info("Processing year %s...", str(year))
            yearfile = self.get_filename(var, year=year)

            # Check if yearly file exists
            if self.validate(yearfile):
                if not overwrite:
                    self.logger.info("Yearly file %s already exists, skipping...", yearfile)
                    continue
                self.logger.warning("Yearly file %s already exists, overwriting...", yearfile)

            year_data = data.sel(time=data.time.dt.year == year)
            # Preserve attributes after slicing
            year_data.attrs.update(original_attrs)

            # Split into months
            months = sorted(set(year_data.time.dt.month.values))
            if performance_reporting:
                months = [months[0]]

            for month in months:
                self.logger.info("Processing month %s...", str(month))
                outfile = self.get_filename(var, year=year, month=month)

                # Check if monthly file exists
                if self.validate(outfile):
                    if not overwrite:
                        self.logger.info("Monthly file %s already exists, skipping...", outfile)
                        continue
                    self.logger.warning("Monthly file %s already exists, overwriting...", outfile)

                month_data = year_data.sel(time=year_data.time.dt.month == month)
                # Preserve attributes after slicing
                month_data.attrs.update(original_attrs)

                # Write file
                if definitive:
                    tmpfile = self.get_filename(var, year=year, month=month, tmp=True)
                    t_start = time()
                    success = self._write_monthly_chunk(
                        month_data, var, year, month, dask=dask, performance_reporting=performance_reporting
                    )
                    t_elapsed = time() - t_start
                    self.logger.info("Chunk execution time: %.2f", t_elapsed)

                    if not success:
                        self.logger.error("Failed to write chunk for %s-%s", year, month)
                        continue

                    # Validate temp file
                    if not self.validate(tmpfile):
                        self.logger.error("Something has gone wrong in %s!", tmpfile)
                        continue

                    # Move IMMEDIATELY (NetCDF timing, not Zarr's deferred move)
                    self.logger.info("Moving temporary file %s to %s", tmpfile, outfile)
                    move_tmp_files(self.tmpdir, self.outdir)

                del month_data

            del year_data

            # Concatenate into yearly file if concat enabled
            if definitive and self._should_concat():
                self._concat_year_files(var, year)

        return True

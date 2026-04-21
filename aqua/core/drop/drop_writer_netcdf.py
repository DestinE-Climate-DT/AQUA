"""
NetCDF Writer for DROP

Handles writing climate data chunks to NetCDF files with optional concatenation
into yearly files. Supports both xarray and CDO for file concatenation.
"""

import glob
import os
import shutil
import subprocess
from time import time

import numpy as np
import pandas as pd
import xarray as xr
from dask.diagnostics import ProgressBar
from dask.distributed import progress
from dask.distributed.diagnostics import MemorySampler

from aqua.core.drop.drop_util import move_tmp_files
from aqua.core.logger import log_configure
from aqua.core.util.io_util import file_is_complete


class NetCDFWriter:
    """
    Writer for NetCDF format in DROP processing.

    Handles monthly file creation and optional yearly concatenation.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        time_encoding,
        var_encoding,
        compact="xarray",
        cdo_options=None,
        dask_client=None,
        performance_reporting=False,
        filename_builder=None,
        loglevel="WARNING",
    ):
        """
        Initialize NetCDF writer.

        Args:
            tmpdir: Temporary directory for intermediate files
            outdir: Output directory for final files
            time_encoding: Encoding dict for time coordinate
            var_encoding: Encoding dict for data variables
            compact: Concatenation method ('xarray', 'cdo', or None)
            cdo_options: List of CDO options for concatenation
            dask_client: Dask client for distributed computing
            performance_reporting: Enable performance reporting
            filename_builder: OutputPathBuilder instance for filename generation
            loglevel: Logging level
        """
        self.tmpdir = tmpdir
        self.outdir = outdir
        self.time_encoding = time_encoding
        self.var_encoding = var_encoding
        self.compact = compact
        self.cdo_options = cdo_options or ["-f", "nc4", "-z", "zip_1"]
        self.dask_client = dask_client
        self.performance_reporting = performance_reporting
        self.filename_builder = filename_builder
        self.logger = log_configure(loglevel, "NetCDFWriter")

    def get_extension(self):
        """Return file extension for this format."""
        return ".nc"

    def validate(self, filepath):
        """
        Validate NetCDF file integrity.

        Args:
            filepath: Path to NetCDF file to validate

        Returns:
            bool: True if file is valid, False otherwise
        """
        return file_is_complete(filepath, loglevel=self.logger.level)

    def write_chunk(self, data, outfile):
        """
        Write a single chunk of data to NetCDF file.

        Args:
            data: xarray Dataset/DataArray to write
            outfile: Output file path
        """
        # File to be written
        if os.path.exists(outfile):
            os.remove(outfile)
            self.logger.warning("Overwriting file %s...", outfile)

        self.logger.info("Computing to write file %s...", outfile)

        # Compute + progress monitoring
        if self.dask_client is not None:
            if self.performance_reporting:
                # Full Dask dashboard to HTML - not implemented in write_chunk
                # This would be handled at a higher level
                job = data.persist()
                progress(job)
                job = job.compute()
            else:
                # Memory monitoring always on
                ms = MemorySampler()
                with ms.sample("chunk"):
                    job = data.persist()
                    progress(job)
                    job = job.compute()
                array_data = np.array(ms.samples["chunk"])
                avg_mem = np.mean(array_data[:, 1]) / 1e9
                max_mem = np.max(array_data[:, 1]) / 1e9
                self.logger.info("Avg memory used: %.2f GiB, Peak memory used: %.2f GiB", avg_mem, max_mem)
        else:
            with ProgressBar():
                job = data.compute()

        # Final safe NetCDF write (serial, no dask)
        job.to_netcdf(
            outfile,
            encoding={"time": self.time_encoding, data.name: self.var_encoding},
        )
        del job
        self.logger.info("Writing file %s successful!", outfile)

    def concat_year_files(self, var, year, get_filename_fn):
        """
        Concatenate monthly files into a single yearly file.

        Args:
            var: Variable name
            year: Year to concatenate
            get_filename_fn: Function to generate filenames
                Should accept (var, year=None, month=None)
        """
        infiles_pattern = get_filename_fn(var, year, month="??")
        monthly_files = sorted(glob.glob(infiles_pattern))

        if len(monthly_files) != 12:
            self.logger.debug("Found %d monthly files for %s year %s, skipping concatenation", len(monthly_files), var, year)
            return

        self.logger.info("Creating a single file for %s, year %s...", var, str(year))
        outfile = get_filename_fn(var, year)
        tmp_outfile = os.path.join(self.tmpdir, os.path.basename(outfile))

        # Move monthly files to tmp for safety
        for monthly_file in monthly_files:
            shutil.move(monthly_file, self.tmpdir)

        # Clean any existing output files
        for f in [tmp_outfile, outfile]:
            if os.path.exists(f):
                os.remove(f)

        # Get the moved files in tmpdir - they keep the same basename
        tmp_monthly_files = [os.path.join(self.tmpdir, os.path.basename(f)) for f in monthly_files]

        # Concatenation with CDO or Xarray
        if self.compact == "cdo":
            command = ["cdo", *self.cdo_options, "cat", *tmp_monthly_files, tmp_outfile]
            self.logger.debug("Using CDO command: %s", command)
            subprocess.check_output(command, stderr=subprocess.STDOUT)
        else:
            self.logger.debug("Using xarray to concatenate files")
            xfield = xr.open_mfdataset(tmp_monthly_files, combine="by_coords", parallel=True)
            name = list(xfield.data_vars)[0]
            xfield.to_netcdf(tmp_outfile, encoding={"time": self.time_encoding, name: self.var_encoding})

        # Move back the yearly file and cleanup
        shutil.move(tmp_outfile, outfile)
        for tmp_file in tmp_monthly_files:
            self.logger.info("Cleaning %s...", tmp_file)
            os.remove(tmp_file)

    def finalize(self):
        """Finalization step (no-op for NetCDF)."""
        self.logger.debug("NetCDF writer finalization complete (no action needed)")

    def get_filename(self, var, year=None, month=None, tmp=False):
        """
        Generate NetCDF filename (monthly or yearly).

        Args:
            var: Variable name
            year: Year (for yearly or monthly files)
            month: Month (for monthly files, optional)
            tmp: If True, return path in tmpdir

        Returns:
            str: Full path to file
        """
        if self.filename_builder:
            filename = self.filename_builder.build_filename(var=var, year=year, month=month)
        else:
            # Fallback simple naming
            if month:
                filename = f"{var}_{year}{month:02d}.nc"
            elif year:
                filename = f"{var}_{year}.nc"
            else:
                filename = f"{var}.nc"

        if tmp:
            return os.path.join(self.tmpdir, os.path.basename(filename))
        else:
            return os.path.join(self.outdir, filename) if not os.path.isabs(filename) else filename

    def check_integrity(self, var, overwrite=False):
        """
        Check integrity of NetCDF files for a variable.

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

        # Check yearly files
        yearfiles = self.get_filename(var, year="*")
        yearfiles = glob.glob(yearfiles)

        if not yearfiles:
            return {"complete": False, "last_record": None, "message": "No files found"}

        checks = [self.validate(yearfile) for yearfile in yearfiles]
        all_checks_true = all(checks)

        if all_checks_true:
            try:
                ds = xr.open_mfdataset(yearfiles)
                last_record = ds.time[-1].values
                last_record_str = pd.to_datetime(last_record).strftime("%Y%m%d")
                return {"complete": True, "last_record": last_record_str, "message": f"All {len(yearfiles)} files complete"}
            except Exception as e:
                return {"complete": False, "last_record": None, "message": f"Error reading files: {e}"}
        else:
            return {"complete": False, "last_record": None, "message": f"{sum(checks)}/{len(checks)} files complete"}

    def write_variable(self, data, var, overwrite=False, definitive=True, performance_reporting=False, history_callback=None):
        """
        Write complete variable with all year/month logic.

        Args:
            data: xarray DataArray with processed data
            var: Variable name
            overwrite: Overwrite existing files
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
            yearfile = self.get_filename(var, year=year)

            # Check if yearly file exists
            if self.validate(yearfile):
                if not overwrite:
                    self.logger.info("Yearly file %s already exists, skipping...", yearfile)
                    continue
                self.logger.warning("Yearly file %s already exists, overwriting...", yearfile)

            year_data = data.sel(time=data.time.dt.year == year)

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

                # Apply history if callback provided
                if history_callback:
                    month_data = history_callback(month_data)

                # Write file
                if definitive:
                    tmpfile = self.get_filename(var, year=year, month=month, tmp=True)
                    t_start = time()
                    self.write_chunk(month_data, tmpfile)
                    t_elapsed = time() - t_start
                    self.logger.info("Chunk execution time: %.2f", t_elapsed)

                    # Validate temp file
                    if not self.validate(tmpfile):
                        self.logger.error("Something has gone wrong in %s!", tmpfile)

                    self.logger.info("Moving temporary file %s to %s", tmpfile, outfile)
                    move_tmp_files(self.tmpdir, self.outdir)

                del month_data

            del year_data

            # Concatenate into yearly file if compact enabled
            if definitive and self.compact:
                self.concat_year_files(var, year, self.get_filename)

        return True

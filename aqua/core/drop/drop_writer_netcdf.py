"""
NetCDF Writer for DROP

Handles writing climate data chunks to NetCDF files with optional concatenation
into yearly files. Supports both xarray and CDO for file concatenation.
"""

import glob
import os
import shutil
import subprocess

import numpy as np
import xarray as xr

from aqua.core.drop.drop_writer_base import BaseWriter
from aqua.core.util.io_util import file_is_complete

# default encoding for netCDF files
TIME_ENCODING = {"units": "days since 1850-01-01 00:00:00", "calendar": "standard", "dtype": "float64"}
VAR_ENCODING = {"dtype": "float64", "zlib": True, "complevel": 1, "_FillValue": np.nan}

# default CDO options for concatenation
CDO_OPTIONS = ["-f", "nc4", "-z", "zip_1"]  # default CDO options for NetCDF output


class NetCDFWriter(BaseWriter):
    """
    Writer for NetCDF format in DROP processing.

    Handles daily or monthly file creation and optional yearly concatenation.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        compact="xarray",
        save_frequency="monthly",
        **kwargs,
    ):
        """
        Initialize NetCDF writer.

        Args:
            tmpdir: Temporary directory for intermediate files
            outdir: Output directory for final files
            compact: Concatenation method ('xarray', 'cdo', or None)
            save_frequency: Checkpoint granularity ('monthly' or 'daily'). Default 'monthly'.
            **kwargs: Additional arguments passed to BaseWriter
        """
        super().__init__(tmpdir, outdir, save_frequency=save_frequency, **kwargs)
        self.time_encoding = TIME_ENCODING
        self.var_encoding = VAR_ENCODING
        self.compact = compact
        self.cdo_options = CDO_OPTIONS

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

    def _get_encoding(self, data, var=None):
        """
        Get NetCDF encoding configuration.

        Args:
            data: xarray Dataset/DataArray
            var: Variable name

        Returns:
            dict: Encoding configuration
        """
        data_vars = list(data.data_vars) if isinstance(data, xr.Dataset) else [data.name]
        var_name = var if var in data_vars else data_vars[0]
        return {"time": self.time_encoding, var_name: self.var_encoding}

    def _write_chunk_to_disk(self, data, tmpfile, encoding):
        """
        Write data chunk to NetCDF file.

        Args:
            data: xarray Dataset/DataArray (already computed)
            tmpfile: Path to temporary file
            encoding: Encoding configuration

        Returns:
            bool: True if write successful
        """
        try:
            data.to_netcdf(tmpfile, encoding=encoding)
            return True
        except Exception as e:
            self.logger.error("NetCDF write failed: %s", e)
            return False

    def _should_concat(self):
        """Check if concatenation is enabled."""
        return self.compact is not None

    def _open_files(self, filepaths):
        """Open one or more NetCDF files."""
        return xr.open_mfdataset(filepaths, combine="by_coords", parallel=True)

    def concat_year_files(self, var, year):
        """
        Concatenate monthly files into a single yearly file.

        NetCDF requires exactly 12 monthly files for concatenation.

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        # Prepare monthly files for concatenation (gather, move to tmp, clean year files)
        tmp_monthly_files, year_file, tmp_year_file = self._prepare_concat_monthly_files(var, year, minimum_required=12)

        if tmp_monthly_files is None:
            return False

        # Concatenate using CDO or xarray
        try:
            if self.compact == "cdo":
                command = ["cdo", *self.cdo_options, "cat", *tmp_monthly_files, tmp_year_file]
                self.logger.debug("Using CDO: %s", " ".join(command[:4]) + " ...")
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            else:
                self.logger.debug("Using xarray for concatenation")
                # Reuse class method for opening files
                ds = self._open_files(tmp_monthly_files)
                var_name = list(ds.data_vars)[0]
                ds.to_netcdf(
                    tmp_year_file,
                    encoding={"time": self.time_encoding, var_name: self.var_encoding},
                )

            # Move yearly file to output and cleanup monthly files
            shutil.move(tmp_year_file, year_file)
            self._cleanup_monthly_files(tmp_monthly_files)
            return True

        except Exception as e:
            self.logger.error("Failed to concatenate monthly files: %s", e)
            return False

    def concat_month_files(self, var, year, month):
        """
        Concatenate daily files into a single monthly file.

        Used when ``save_frequency='daily'``.  Expects one file per calendar day
        present in the data for the given year-month; accepts any number >= 1.

        Args:
            var: Variable name
            year: Year
            month: Month

        Returns:
            bool: True if successful
        """
        # Gather daily files for this month using a glob on the day component
        daily_pattern = self.get_filename(var, year=year, month=month, day="??")
        daily_files = sorted(glob.glob(daily_pattern))

        if not daily_files:
            self.logger.warning("No daily files found for %s year %s month %s", var, year, month)
            return False

        self.logger.info(
            "Concatenating %d daily files into monthly file for %s %s-%02d...",
            len(daily_files),
            var,
            year,
            month,
        )

        month_file = self.get_filename(var, year=year, month=month)
        tmp_month_file = os.path.join(self.tmpdir, os.path.basename(month_file))

        # Move daily files to tmpdir for safety
        for daily_file in daily_files:
            shutil.move(daily_file, self.tmpdir)
        tmp_daily_files = [os.path.join(self.tmpdir, os.path.basename(f)) for f in daily_files]

        # Remove any pre-existing monthly file
        for f in [tmp_month_file, month_file]:
            if os.path.exists(f):
                os.remove(f)

        try:
            if self.compact == "cdo":
                command = ["cdo", *self.cdo_options, "cat", *tmp_daily_files, tmp_month_file]
                self.logger.debug("Using CDO: %s", " ".join(command[:4]) + " ...")
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            else:
                self.logger.debug("Using xarray for daily-to-monthly concatenation")
                ds = self._open_files(tmp_daily_files)
                var_name = list(ds.data_vars)[0]
                ds.to_netcdf(
                    tmp_month_file,
                    encoding={"time": self.time_encoding, var_name: self.var_encoding},
                )

            shutil.move(tmp_month_file, month_file)
            self._cleanup_monthly_files(tmp_daily_files)
            return True

        except Exception as e:
            self.logger.error("Failed to concatenate daily files into monthly file: %s", e)
            return False

"""
NetCDF Writer for DROP

Handles writing climate data chunks to NetCDF files with optional concatenation
into yearly files. Supports both xarray and CDO for file concatenation.
"""

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

    Handles monthly file creation and optional yearly concatenation.
    """

    def __init__(
        self,
        tmpdir,
        outdir,
        compact="xarray",
        **kwargs,
    ):
        """
        Initialize NetCDF writer.

        Args:
            tmpdir: Temporary directory for intermediate files
            outdir: Output directory for final files
            compact: Concatenation method ('xarray', 'cdo', or None)
            **kwargs: Additional arguments passed to BaseWriter
        """
        super().__init__(tmpdir, outdir, **kwargs)
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
        if var:
            return {"time": self.time_encoding, var: self.var_encoding}
        # Fallback: use first data variable
        var_name = list(data.data_vars)[0] if isinstance(data, xr.Dataset) else data.name
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

    def _open_single_file(self, filepath):
        """Open a single NetCDF file."""
        return xr.open_dataset(filepath)

    def _open_multiple_files(self, filepaths):
        """Open multiple NetCDF files."""
        return xr.open_mfdataset(filepaths, combine="by_coords", parallel=True)

    def _concat_year_files(self, var, year):
        """
        Concatenate monthly files into a single yearly file.

        NetCDF requires exactly 12 monthly files for concatenation.

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        # Get and validate monthly files (NetCDF requires exactly 12)
        monthly_files, is_valid = self._get_and_validate_monthly_files(var, year, minimum_required=12)

        if not is_valid:
            return False

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
                os.remove(f)

        # Get the moved files in tmpdir - they keep the same basename
        tmp_monthly_files = [os.path.join(self.tmpdir, os.path.basename(f)) for f in monthly_files]

        # Concatenate using CDO or xarray
        try:
            if self.compact == "cdo":
                command = ["cdo", *self.cdo_options, "cat", *tmp_monthly_files, tmp_year_file]
                self.logger.debug("Using CDO: %s", " ".join(command[:4]) + " ...")
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            else:
                self.logger.debug("Using xarray for concatenation")
                # Reuse class method for opening multiple files
                ds = self._open_multiple_files(tmp_monthly_files)
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

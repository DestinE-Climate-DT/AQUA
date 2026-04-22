"""
NetCDF Writer for DROP

Handles writing climate data chunks to NetCDF files with optional concatenation
into yearly files. Supports both xarray and CDO for file concatenation.
"""

import glob
import os
import shutil
import subprocess

import xarray as xr

from aqua.core.drop.drop_writer_base import BaseWriter
from aqua.core.util.io_util import file_is_complete


class NetCDFWriter(BaseWriter):
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
        **kwargs,
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
            **kwargs: Additional arguments passed to BaseWriter
        """
        super().__init__(tmpdir, outdir, **kwargs)
        self.time_encoding = time_encoding
        self.var_encoding = var_encoding
        self.compact = compact
        self.cdo_options = cdo_options or ["-f", "nc4", "-z", "zip_1"]

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

        Args:
            var: Variable name
            year: Year to concatenate

        Returns:
            bool: True if successful
        """
        infiles_pattern = self.get_filename(var, year, month="??")
        monthly_files = sorted(glob.glob(infiles_pattern))

        if len(monthly_files) == 0:
            self.logger.debug("No monthly files found for %s year %s", var, year)
            return False

        if len(monthly_files) != 12:
            self.logger.debug(
                "Found %d monthly files for %s year %s (expected 12), skipping concatenation",
                len(monthly_files),
                var,
                year,
            )
            return False

        self.logger.info("Creating a single file for %s, year %s...", var, str(year))
        outfile = self.get_filename(var, year)
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
        try:
            if self.compact == "cdo":
                command = ["cdo", *self.cdo_options, "cat", *tmp_monthly_files, tmp_outfile]
                self.logger.debug("Using CDO command: %s", command)
                subprocess.check_output(command, stderr=subprocess.STDOUT)
            else:
                self.logger.debug("Using xarray to concatenate files")
                xfield = xr.open_mfdataset(tmp_monthly_files, combine="by_coords", parallel=True)
                name = list(xfield.data_vars)[0]
                xfield.to_netcdf(
                    tmp_outfile,
                    encoding={"time": self.time_encoding, name: self.var_encoding},
                )

            # Move back the yearly file and cleanup
            shutil.move(tmp_outfile, outfile)
            for tmp_file in tmp_monthly_files:
                self.logger.info("Cleaning %s...", tmp_file)
                os.remove(tmp_file)
            return True

        except Exception as e:
            self.logger.error("Failed to concatenate year files: %s", e)
            return False

"""Utility module for DROP"""

import os
import shutil

import zarr


class WriteReport:
    """
    Collect what happens to the existing and to the new yearly/monthly files while writing a variable,
    to log a final summary. Periods are reported once and never retried within the run.

    Attributes:
        invalid (list): existing files failing validation, to be regenerated.
        failed (list): periods that could not be produced, as "period (reason)".
    """

    def __init__(self, var, definitive):
        self.var = var
        self.definitive = definitive
        self.invalid = []
        self.failed = []

    @staticmethod
    def _period(year, month=None):
        return str(year) if month is None else f"{year}-{int(month):02d}"

    def add_invalid(self, year, month=None, note=None):
        """Register an existing file failing validation."""
        period = self._period(year, month)
        self.invalid.append(f"{period} ({note})" if note else period)

    def add_failed(self, year, reason, month=None):
        """Register a period that could not be produced."""
        self.failed.append(f"{self._period(year, month)} ({reason})")

    def log(self, logger):
        """Log the final summary: invalid files (dry run only, otherwise they are regenerated) and failures."""
        if self.invalid and not self.definitive:
            logger.warning(
                "Dry run for %s: %d invalid existing file(s) would be regenerated: %s",
                self.var,
                len(self.invalid),
                ", ".join(self.invalid),
            )
        if self.failed:
            logger.error(
                "Variable %s: %d period(s) could not be produced: %s", self.var, len(self.failed), ", ".join(self.failed)
            )


def move_tmp_files(tmp_directory, output_directory):
    """
    Move temporary files from the tmp directory to the output directory.

    Handles both NetCDF files (.nc) and Zarr stores (.zarr directories).
    Both formats now use tmpdir for intermediate operations (unified pattern).
    For NetCDF files, removes "_tmp" suffix from filename.
    """
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    for tmp_file in os.listdir(tmp_directory):
        tmp_file_path = os.path.join(tmp_directory, tmp_file)

        # Handle NetCDF files
        if tmp_file.endswith(".nc"):
            if "_tmp" in tmp_file:
                new_file_name = tmp_file.replace("_tmp", "")
            else:
                new_file_name = tmp_file
            new_file_path = os.path.join(output_directory, new_file_name)
            shutil.move(tmp_file_path, new_file_path)

        # Handle Zarr stores (directories ending with .zarr)
        elif tmp_file.endswith(".zarr") and os.path.isdir(tmp_file_path):
            new_file_path = os.path.join(output_directory, tmp_file)
            # Remove existing store in output if present
            if os.path.exists(new_file_path):
                shutil.rmtree(new_file_path)
            shutil.move(tmp_file_path, new_file_path)


def zarr_encoding(chunks, compressor_level=1):
    """
    Build zarr v3 encoding with gzip compression and explicit on-disk chunks.

    Shared by ZarrWriter and IcechunkWriter.

    Args:
        chunks (dict): Chunk sizes for each variable, as returned by BaseWriter._disk_chunks.
            Variables with None chunks use the zarr default chunking.
        compressor_level (int): Compression level for GzipCodec (1-9). Defaults to 1.

    Returns:
        dict: Encoding configuration or None if chunks is empty.
    """
    compressor = zarr.codecs.GzipCodec(level=compressor_level)
    encoding = {}
    for var_name, var_chunks in chunks.items():
        encoding[var_name] = {"compressors": (compressor,)}
        if var_chunks:
            encoding[var_name]["chunks"] = var_chunks
    return encoding or None

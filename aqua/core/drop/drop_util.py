"""Utility module for DROP"""

import math
import os
import shutil

import zarr
from smmregrid import GridType


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


def zarr_encoding(chunks, shards=None, compressor_level=1):
    """
    Build zarr v3 encoding with gzip compression, explicit on-disk chunks and optional shards.

    Shared by ZarrWriter and IcechunkWriter.

    Args:
        chunks (dict): Chunk sizes for each variable, as returned by BaseWriter._disk_chunks.
            Variables with None chunks use the zarr default chunking.
        shards (dict, optional): Shard sizes for each variable; each shard groups many chunks
            in a single file. Variables missing or with None shards are not sharded. Defaults to None.
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
            if shards and shards.get(var_name):
                encoding[var_name]["shards"] = shards[var_name]
    return encoding or None


def target_chunks(data, target, logger=None):
    """
    Chunks of a variable filling time then vertical dimensions up to target bytes, horizontal dimensions whole.

    Shared by the read chunks of DROP catalog entries and the shards of the ZarrWriter,
    so that each read chunk is exactly one shard on disk.

    Args:
        data (xarray.DataArray): Variable to chunk (only sizes and dtype are used).
        target (int): Target chunk size in bytes.
        logger (logging.Logger, optional): Logger to warn when a horizontal slice exceeds the target.

    Returns:
        dict: Chunk size along each dimension, -1 for horizontal dimensions kept whole.
            Empty if the variable has no time or no horizontal dimension.
    """
    # classify the dimensions with smmregrid: vertical dims are the masked (e.g. depth) and other dims
    gridtype = GridType(dims=data.dims)
    # bounds and other auxiliary variables have no time or no horizontal dimensions
    if not gridtype.time_dims or not gridtype.horizontal_dims:
        return {}
    vertical = ([gridtype.mask_dim] if gridtype.mask_dim else []) + gridtype.other_dims

    # horizontal dims are never split, to stay aligned with the on-disk chunks (one horizontal slice)
    chunks = dict.fromkeys(gridtype.horizontal_dims, -1)
    slice_bytes = data.dtype.itemsize * math.prod(data.sizes[dim] for dim in gridtype.horizontal_dims)

    # number of horizontal slices fitting in a chunk
    nslices = target // slice_bytes
    if nslices < 1 and logger:
        logger.warning("Horizontal slice of %s (%d bytes) exceeds the chunk target (%d bytes)", data.name, slice_bytes, target)

    for dim in gridtype.time_dims + vertical:
        size = min(data.sizes[dim], max(nslices, 1))
        chunks[dim] = size
        nslices //= size

    return chunks

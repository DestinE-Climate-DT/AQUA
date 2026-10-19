"""Utility module for DROP"""

import os
import shutil

import zarr


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

"""Utility module for DROP"""

import os
import shutil

from pandas.tseries.frequencies import to_offset

from aqua.core.util import frequency_string_to_pandas


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


def estimate_time_chunk_size(freq_str, buffer=1.2):
    """
    Estimate zarr time chunk size from an AQUA frequency string.

    Converts the frequency to a pandas offset via
    :func:`frequency_string_to_pandas`, then computes how many
    timesteps fit in a 31-day month (worst case). A safety buffer
    is applied so chunk boundaries never fall mid-month.

    Args:
        freq_str (str): AQUA frequency string (e.g., 'daily', 'monthly', '6hourly').
        buffer (float, optional): Multiplicative safety factor. Defaults to 1.2.

    Returns:
        int: Recommended chunk size for the time dimension.

    Examples:
        >>> estimate_time_chunk_size("daily")
        37
        >>> estimate_time_chunk_size("hourly")
        892
    """

    pandas_freq = frequency_string_to_pandas(freq_str)
    try:
        offset = to_offset(pandas_freq)
        try:
            # Fixed-interval offsets (hour, day, minute, second) expose .nanos
            seconds_per_step = offset.nanos / 1e9
            max_steps_per_month = 31 * 24 * 3600 / seconds_per_step
        except ValueError:
            # Calendar offsets (monthly, quarterly, yearly) have variable length
            freqstr = (offset.freqstr or "").upper()
            if any(c in freqstr for c in ("Y", "A")):
                max_steps_per_month = 1  # yearly: ~1/12 steps per month
            elif "Q" in freqstr:
                max_steps_per_month = 3  # quarterly
            else:
                max_steps_per_month = 12  # monthly (default for calendar offsets)
    except Exception:
        max_steps_per_month = 1000  # conservative fallback for unknown frequencies

    return int(max_steps_per_month * buffer)

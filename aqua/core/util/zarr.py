"""Zarr reference module"""

import json
import os

import xarray as xr
from kerchunk.combine import MultiZarrToZarr
from kerchunk.hdf import SingleHdf5ToZarr

from aqua.core.logger import log_configure


def create_zarr_reference(filelist, outfile, loglevel="WARNING"):
    """
    Create a Zarr file from a list of HDF5/NetCDF files.

    Args:
        filelist (list): A list of file paths to HDF5 files.
        outfile (str): The path to the output Zarr file.
        loglevel (str, optional): The log level for logging. Defaults to 'WARNING'.

    Returns:
        None
    """

    logger = log_configure(log_level=loglevel, log_name="Zarr reference creator")
    data = xr.open_mfdataset(filelist, combine="by_coords")
    identical_coords = [coord for coord in data.coords if coord != "time"]
    logger.debug("Common coordinates: %s", identical_coords)

    logger.debug("Creating Zarr file from %s", filelist)
    singles = [SingleHdf5ToZarr(filepath, inline_threshold=0).translate() for filepath in sorted(filelist)]

    logger.debug("Combining Zarr files")
    mzz = MultiZarrToZarr(
        singles,
        concat_dims=["time"],
        identical_dims=identical_coords,
    )

    logger.debug("Translating Zarr files to json")
    try:
        out = mzz.translate()
    except ValueError as e:
        logger.error("Cannot create Zarr %s file due chunk mismatch", outfile)
        logger.error(e)
        return None

    # Dump to file
    logger.info("Dumping to file JSON %s", outfile)
    if os.path.exists(outfile):
        os.remove(outfile)
    with open(outfile, "w") as file:
        json.dump(out, file)

    return outfile

def get_kerchunk_cache_dir(filelist, configdir, model, exp, source):
    import hashlib
    from pathlib import Path
    key = hashlib.md5("\n".join(sorted(map(str, filelist))).encode()).hexdigest()[:12]
    cache = Path(configdir) / "kerchunk_cache" / f"{model}_{exp}_{source}_{key}"
    cache.mkdir(parents=True, exist_ok=True)
    return str(cache)

def create_single_zarr_reference(filepath, outfile, loglevel="WARNING"):
    """One NetCDF4/HDF5 file → one kerchunk JSON."""
    ref = SingleHdf5ToZarr(filepath, inline_threshold=0).translate()
    with open(outfile, "w") as f:
        json.dump(ref, f)
    return outfile
    
def open_zarr_reference(json_path, chunks=None):
    """Open kerchunk JSON as xarray Dataset (virtual Zarr)."""
    import fsspec
    mapper = fsspec.get_mapper(
        "reference://",
        fo=json_path,
        target_protocol="file",
        remote_protocol="file",
    )
    return xr.open_dataset(
        mapper,
        engine="zarr",
        consolidated=False,
        chunks=chunks if chunks is not None else {},
    )

def open_netcdf_files_via_kerchunk(filelist, cache_dir, loglevel="WARNING", chunks=None):
    os.makedirs(cache_dir, exist_ok=True)
    datasets = []
    for f in sorted(filelist):
        json_path = os.path.join(cache_dir, os.path.basename(f) + ".json")
        if not os.path.exists(json_path):
            create_single_zarr_reference(f, json_path, loglevel=loglevel)
        datasets.append(open_zarr_reference(json_path, chunks=chunks))
    return xr.merge(datasets) if len(datasets) > 1 else datasets[0]

    
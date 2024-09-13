"""Zarr reference module"""

import os
import json
import xarray as xr
from kerchunk.hdf import SingleHdf5ToZarr
from kerchunk.combine import MultiZarrToZarr
from aqua.logger import log_configure


def create_zarr_reference(filelist, outfile, loglevel='WARNING'):
    """
    Create a Zarr file from a list of HDF5/NetCDF files.

    Args:
        filelist (list): A list of file paths to HDF5 files.
        outfile (str): The path to the output Zarr file.
        loglevel (str, optional): The log level for logging. Defaults to 'WARNING'.

    Returns:
        None
    """

    logger = log_configure(log_level=loglevel, log_name='Zarr reference creator')
    data = xr.open_mfdataset(filelist, combine='by_coords')
    identical_coords = [coord for coord in data.coords if coord != 'time']
    logger.debug('Common coordinates: %s', identical_coords)

    logger.debug('Creating Zarr file from %s', filelist)
    singles = [SingleHdf5ToZarr(filepath, inline_threshold=0).translate() for filepath in sorted(filelist)]

    logger.debug('Combining Zarr files')
    mzz = MultiZarrToZarr(
        singles,
        concat_dims=["time"],
        identical_dims=identical_coords,
    )

    logger.debug('Translating Zarr files to json')
    try:
        out = mzz.translate()
    except ValueError as e:
        logger.error('Cannot create Zarr %s file due chunk mismatch', outfile)
        logger.error(e)
        return None

    # Dump to file
    logger.info('Dumping to file JSON %s', outfile)
    if os.path.exists(outfile):
        os.remove(outfile)
    with open(outfile, "w") as file:
        json.dump(out, file)

    return outfile

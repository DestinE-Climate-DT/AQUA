"""Contains some functions external to the TCs class (defined in the tropical_cyclones.py file)"""

import os
from dask.distributed import progress
from dask.diagnostics import ProgressBar


def clean_files(filelist):
    """
    Removes the specified files from the filesystem.

    Args:
        filelist (str or list): A single filename or a list of filenames to be removed.

    Returns:
        None
    """

    if isinstance(filelist, str):
        filelist = [filelist]

    for fileout in filelist:
        if os.path.exists(fileout):
            os.remove(fileout)


def lonlatbox(lon, lat, delta):
    """
    Define the list for the box to retain high res data in the vicinity of the TC centres.

    Args:
        lon: longitude of the TC centre
        lat: latitude of the TC centre
        delta: length in degrees of the lat lon box

    Returns:
        box: list with the box coordinates
    """

    return [float(lon) - delta, float(lon) + delta, float(lat) - delta, float(lat) + delta]


def write_fullres_field(gfield, filestore, dask):
    """
    Writes the high resolution file (netcdf) format with values only within the TCs centres box.

    Args:
        gfield: field to write
        filestore: file to save
        dask: if dask is active or not

    Returns:
        None
    """

    time_encoding = {
        'time':
        {
            'units': 'days since 1970-01-01',
            'calendar': 'standard',
            'dtype': 'float64'
        }
    }
    single_var_encoding = {
        "zlib": True, "complevel": 1
    }
    var_encoding = {var: single_var_encoding for var in gfield.data_vars}
    final_encoding = {**time_encoding, **var_encoding}

    if isinstance(gfield, int):
        print("No tracks to write")
    else:
        gfield = gfield.where(gfield != 0)
        save_file = gfield.to_netcdf(filestore,
                                     encoding=final_encoding,
                                     compute=False)

        if dask:
            w_job = save_file.persist()
            progress(w_job)
            del w_job
        else:
            with ProgressBar():
                save_file.compute()
        # gfield.close()

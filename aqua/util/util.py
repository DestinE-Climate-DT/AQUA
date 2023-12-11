"""Module containing general utility functions for AQUA"""

import os
import random
import string
import logging
import xarray as xr
from aqua.logger import log_configure


def generate_random_string(length):
    """
    Generate a random string of lowercase and uppercase letters and digits
    """

    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string


def get_arg(args, arg, default):
    """
    Support function to get arguments

    Args:
        args: the arguments
        arg: the argument to get
        default: the default value

    Returns:
        The argument value or the default value
    """

    res = getattr(args, arg)
    if not res:
        res = default
    return res


def create_folder(folder, loglevel="WARNING"):
    """
    Create a folder if it does not exist

    Args:
        folder (str): the folder to create
        loglevel (str): the log level

    Returns:
        None
    """
    logger = log_configure(loglevel, 'create_folder')

    if not os.path.exists(folder):
        logger.info('Creating folder %s', folder)
        os.makedirs(folder)
    else:
        logger.info('Folder %s already exists', folder)


def file_is_complete(filename, logger=logging.getLogger()):
    """Basic check to see if file exists and that includes values which are not NaN
    Return a boolean that can be used as a flag for further operation
    True means that we have to re-do the computation
    A logger can be passed for correct logging properties"""

    if os.path.isfile(filename):
        logger.info('File %s is found...', filename)
        try:
            xfield = xr.open_dataset(filename)
            if len(xfield.data_vars) == 0:
                logger.error('File %s is empty! Recomputing...', filename)
                check = False
            else:
                varname = list(xfield.data_vars)[0]
                if xfield[varname].isnull().all():
                    # if xfield[varname].isnull().all(dim=['lon','lat']).all():
                    logger.error('File %s is full of NaN! Recomputing...', filename)
                    check = False
                else:
                    check = True
                    logger.info('File %s seems ok!', filename)
        # we have no clue which kind of exception might show up
        except ValueError:
            logger.info('Something wrong with file %s! Recomputing...', filename)
            check = False
    else:
        logger.info('File %s not found...', filename)
        check = False

    return check


def find_vert_coord(ds):
    """
    Identify the vertical coordinate name(s) based on coordinate units. Returns always a list.
    The list will be empty if none found.
    """
    vert_coord = [x for x in ds.coords if ds.coords[x].attrs.get("units") in ["Pa", "hPa", "m", "km", "Km", "cm", ""] ]
    return vert_coord

"""Module containing general utility functions for AQUA"""

import os
import random
import string
import yaml
import re
import numpy as np
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
        os.makedirs(folder, exist_ok=True)
    else:
        logger.info('Folder %s already exists', folder)


def file_is_complete(filename, loglevel='WARNING'):
    """Basic check to see if file exists and that includes values which are not NaN
    Return a boolean that can be used as a flag for further operation
    True means that we have to re-do the computation
    A logger can be passed for correct logging properties"""

    logger = log_configure(loglevel, 'file_is_complete')

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
                    logger.error('File %s is full of NaN! Recomputing...', filename)
                    check = False   
                else:
                    mydims = [dim for dim in xfield[varname].dims if dim != 'time']
                    nan_count = np.isnan(xfield[varname]).sum(dim=mydims)
                    check = all(value == nan_count[0] for value in nan_count)
                    if check: 
                        logger.info('File %s seems ok!', filename)
                    else:
                        logger.error('File %s has at least one time step with NaN! Recomputing...', filename)
                        
        # we have no clue which kind of exception might show up
        except ValueError:
            logger.error('Something wrong with file %s! Recomputing...', filename)
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
    vert_coord = [x for x in ds.coords if ds.coords[x].attrs.get("units") in ["Pa", "hPa", "m", "km", "Km", "cm", ""]]
    return vert_coord


def extract_literal_and_numeric(text):
    """
    Given a string, extract its literal and numeric part
    """
    # Using regular expression to find alphabetical characters and digits in the text
    match = re.search(r'(\d*)([A-Za-z]+)', text)
    
    if match:
        # If a match is found, return the literal and numeric parts
        literal_part = match.group(2)
        numeric_part = match.group(1)
        if not numeric_part:
            numeric_part = 1
        return literal_part, int(numeric_part)
    else:
        # If no match is found, return None or handle it accordingly
        return None, None

def get_aqua_path():
    """
    Retrieves the directory path specified by the AQUA environment variable.
    """
    aqua_path = os.getenv('AQUA')
    if aqua_path is None:
        raise ValueError("The AQUA environment variable is not set.")
    return aqua_path

def get_machine():
    """
    Retrieves the machine configuration from a YAML file located in the directory
    specified by the AQUA environment variable.
    """
    # Use the new function to get the config file path
    aqua_path = get_aqua_path()
    # Construct the path to the YAML file
    config_file_path = os.path.join(aqua_path, 'config', 'config-aqua.yaml')

    try:
        # Attempt to open and read the YAML file
        with open(config_file_path, 'r') as file:
            config = yaml.safe_load(file)
    except FileNotFoundError:
        raise FileNotFoundError(f"YAML file not found at path: {config_file_path}")

    # Extract the machine configuration
    try:
        machine = config['machine']
    except KeyError:
        raise KeyError("'machine' key not found in the YAML configuration.")

    return machine

def username():
    """
    Retrieves the current user's username from the 'USER' environment variable.
    """
    user = os.getenv('USER')
    if user is None:
        raise EnvironmentError("The 'USER' environment variable is not set.")
    return user 

"""Module containing general utility functions for AQUA"""

import os
import random
import string
import re
import numpy as np
import xarray as xr
from pypdf import PdfReader, PdfWriter
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


def add_pdf_metadata(filename: str,
                     metadata_value: str,
                     metadata_name: str = 'Description',
                     old_metadata: bool = True):
    """
    Open a pdf and add a new metadata.

    Args:
        filename (str): the filename of the pdf.
                        It must be a valid full path.
        metadata_value (str): the value of the new metadata
        metadata_name (str): the name of the new metadata.
                            Default is 'Description'
        old_metadata (bool): if True, the old metadata will be kept.
                            Default is True

    Raise:
        FileNotFoundError: if the file does not exist
    """
    if not os.path.isfile(filename):
        raise FileNotFoundError(f'File {filename} not found')
    else:  # File exists
        pdf_reader = PdfReader(filename)
        pdf_writer = PdfWriter()

        # Adding existing pages to the new pdf
        for page in pdf_reader.pages:
            pdf_writer.addpage(page)

        # Keep old metadata if required
        if old_metadata is True:
            metadata = pdf_reader.metadata
            pdf_writer.add_metadata(metadata)

        # Add the new metadata
        pdf_writer.add_metadata({metadata_name: metadata_value})

        # Overwrite input pdf
        with open(filename, 'wb') as f:
            pdf_writer.write(f)
            f.close()

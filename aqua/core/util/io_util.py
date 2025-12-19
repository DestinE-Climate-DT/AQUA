"""Module containing I/O utilities and managing file paths, formats, or metadata fir AQUA."""

import os
import re
import numpy as np
import xarray as xr
import pandas as pd
from glob import glob
from .util import to_list
from pypdf import PdfReader, PdfWriter
from PIL import Image, PngImagePlugin
from aqua.core.logger import log_configure
from aqua.core.version import __version__ as version


def files_exist(path):
    """
    Verify that a list or path includes files
    """

    # Iterate over each pattern and check for the existence of matching files
    for p in to_list(path):
        if glob(p):  # If glob finds at least one match, return True
            return True

    return False


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
    """
    Basic check to see if file exists and that includes values
    which are not NaN in its first variabiles
    Return a boolean that can be used as a flag for further operation
    A loglevel can be passed for tune the logging properties

    Args:
        filename: a string with the filename
        loglevel: the log level

    Returns
        A boolean flag (True for file ok, False for file corrupted)
    """

    logger = log_configure(loglevel, 'file_is_complete')

    # check file existence
    if not os.path.isfile(filename):
        logger.info('File %s not found...', filename)
        return False

    logger.info('File %s is found...', filename)

    # check opening
    try:
        xfield = xr.open_dataset(filename)

        # check variables
        if len(xfield.data_vars) == 0:
            logger.error('File %s has no variables!', filename)
            return False

        # check on a single variable
        varname = list(xfield.data_vars)[0]

        # all NaN case
        if xfield[varname].isnull().all():

            # case of a mindate on all NaN single files
            mindate = xfield[varname].attrs.get('mindate')
            if mindate is not None:
                logger.warning('All NaN and mindate found: %s', mindate)
                if xfield[varname].time.max() < np.datetime64(mindate):
                    logger.info('File %s is full of NaN but it is ok according to mindate', filename)
                    return True

                logger.error('File %s is full of NaN and not ok according to mindate', filename)
                return False

            logger.error('File %s is empty or full of NaN! Recomputing...', filename)
            return False

        # some NaN case
        mydims = [dim for dim in xfield[varname].dims if dim != 'time']
        nan_count = np.isnan(xfield[varname]).sum(dim=mydims)
        if all(value == nan_count[0] for value in nan_count):
            logger.info('File %s seems ok!', filename)
            return True

        # case of a mindate on some NaN
        mindate = xfield[varname].attrs.get('mindate')
        if mindate is not None:
            logger.warning('Some NaN and mindate found: %s', mindate)
            last_nan = xfield.time[np.where(nan_count == nan_count[0])].max()
            if np.datetime64(mindate) > last_nan:
                logger.info('File %s has some of NaN up to %s but it is ok according to mindate %s',
                            filename, last_nan.values, mindate)
                return True

            logger.error('File %s has some NaN bit it is not ok according to mindate', filename)
            return False

        logger.error('File %s has at least one time step with NaN! Recomputing...', filename)
        return False

    except Exception as e:
        logger.error('Something wrong with file %s! Recomputing... Error: %s', filename, e)
        return False


def normalize_key(key: str) -> str:
    """
    Normalize metadata key by removing leading '/' and converting to lowercase.
    """
    return key.lstrip('/').lower()


def normalize_value(value):
    """
    Normalize metadata values. If the value is a dictionary or dictionary-like string, normalize its keys.

    Args:
        value: Metadata value to normalize.

    Returns:
        Normalized value.
    """
    # Check if value is a dictionary, normalize its keys
    if isinstance(value, dict):
        return {normalize_key(k): normalize_value(v) for k, v in value.items()}
    
    # Check if value is a string that looks like a dictionary-like structure
    if isinstance(value, str):
        if re.match(r"^\{.*\}$", value.strip()):
            try:
                # Convert dictionary-like string into an actual dictionary
                parsed_value = eval(value, {"__builtins__": None}, {})
                if isinstance(parsed_value, dict):
                    # Normalize the keys if it is a dictionary
                    return {normalize_key(k): normalize_value(v) for k, v in parsed_value.items()}
            except Exception as e:
                # Log parsing errors and return the original string if parsing fails
                log_configure('WARNING', 'normalize_value').warning(f"Failed to parse string as dictionary: {e}")
    
    # Return the value as-is if it can't be processed further
    return value


def add_pdf_metadata(filename: str,
                     metadata_value: str | dict,
                     metadata_name: str = '/Description',
                     old_metadata: bool = True,
                     loglevel: str = 'WARNING'):
    """
    Open a pdf and add new metadata.

    Args:
        filename (str): the filename of the pdf.
                        It must be a valid full path.
        metadata_value (str | dict): the value(s) of the new metadata.
        metadata_name (str): the name of the new metadata.
                            Default is '/Description'.
        old_metadata (bool): if True, the old metadata will be kept.
                            Default is True.
        loglevel (str): the log level. Default is 'WARNING'.

    Raise:
        FileNotFoundError: if the file does not exist.
    """
    logger = log_configure(loglevel, 'add_pdf_metadata')

    if not os.path.isfile(filename):
        raise FileNotFoundError(f'File {filename} not found')

    # Ensure metadata_name starts with '/'
    if metadata_name and not metadata_name.startswith('/'):
        logger.debug('metadata_name does not start with "/". Adding it...')
        metadata_name = '/' + metadata_name

    pdf_reader = PdfReader(filename)
    pdf_writer = PdfWriter()

    # Add existing pages to the new PDF
    for page in pdf_reader.pages:
        pdf_writer.add_page(page)

    # Keep old metadata if required
    if old_metadata:
        logger.debug('Keeping old metadata')
        metadata = pdf_reader.metadata
        pdf_writer.add_metadata(metadata)

    # Add the new metadata
    if isinstance(metadata_value, dict):
        metadata_value = {f'/{k}' if not k.startswith('/') else k: v for k, v in metadata_value.items()}  # Ensure keys start with '/'
        pdf_writer.add_metadata(metadata_value)
    else:
        pdf_writer.add_metadata({metadata_name: metadata_value})

    # Overwrite input PDF
    with open(filename, 'wb') as f:
        pdf_writer.write(f)


def add_png_metadata(png_path: str, metadata: dict, loglevel: str = 'WARNING'):
    """
    Add metadata to a PNG image file.

    Args:
        png_path (str): The path to the PNG image file.
        metadata (dict): A dictionary of metadata to add to the PNG file.
                         Note: Metadata keys do not need a '/' prefix.
        loglevel (str): The log level. Default is 'WARNING'.
    """
    logger = log_configure(loglevel, 'add_png_metadata')

    if not os.path.isfile(png_path):
        raise FileNotFoundError(f'File {png_path} not found')

    image = Image.open(png_path)

    # Create a dictionary for the PNG metadata
    png_info = PngImagePlugin.PngInfo()

    # Add the new metadata
    for key, value in metadata.items():
        png_info.add_text(key, str(value))
        logger.debug(f'Adding metadata: {key} = {value}')

    # Save the file with the new metadata
    image.save(png_path, "PNG", pnginfo=png_info)
    logger.info(f"Metadata added to PNG: {png_path}")


def update_metadata(metadata: dict = None, additional_metadata: dict = None) -> dict:
    """
    Update the provided metadata dictionary with the current date, time, aqua package version,
    and additional diagnostic information.

    Args:
        metadata (dict, optional): The original metadata dictionary.
        additional_metadata (dict, optional): A dictionary containing additional metadata fields (e.g., diagnostic, model, experiment, etc.).

    Returns:
        dict: The updated metadata dictionary.
    """
    if metadata is None:
        metadata = {}

    # Add current date and time to metadata
    now = pd.Timestamp.now()
    date_now = now.strftime("%Y-%m-%d %H:%M:%S")
    metadata['timestamp'] = date_now
    metadata['aqua_version'] = version

    # Add additional metadata fields
    if additional_metadata:
        metadata.update(additional_metadata)

    return metadata

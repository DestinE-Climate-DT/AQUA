"""Module containing general utility functions for AQUA"""

import os
import random
import string
import re
import sys
import numpy as np
import xarray as xr
import datetime
from glob import glob
from pypdf import PdfReader, PdfWriter
from PIL import Image, PngImagePlugin
from aqua.logger import log_configure
from IPython.display import display, Image as IPImage, FileLink


def generate_random_string(length):
    """
    Generate a random string of lowercase and uppercase letters and digits
    """

    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string


def to_list(arg):

    """Support function to ensure conversion of a variable to list"""

    if isinstance(arg, str):
        arg = [arg]
    return arg

def files_exist(path):
    """
    Verify that a list or path includes files
    """

    # Iterate over each pattern and check for the existence of matching files
    for p in to_list(path):
        if glob(p):  # If glob finds at least one match, return True
            return True

    return False

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
                     metadata_name: str = '/Description',
                     old_metadata: bool = True,
                     loglevel: str = 'WARNING'):
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
        loglevel (str): the log level. Default is 'WARNING'

    Raise:
        FileNotFoundError: if the file does not exist
    """
    logger = log_configure(loglevel, 'add_pdf_metadata')

    if not os.path.isfile(filename):
        raise FileNotFoundError(f'File {filename} not found')

    # Check that metadata_name starts with '/'
    if metadata_name and metadata_name[0] != '/':
        logger.debug('metadata_name does not start with "/". Adding it...')
        metadata_name = '/' + metadata_name

    pdf_reader = PdfReader(filename)
    pdf_writer = PdfWriter()

    # Adding existing pages to the new pdf
    for page in pdf_reader.pages:
        pdf_writer.add_page(page)

    # Keep old metadata if required
    if old_metadata is True:
        logger.debug('Keeping old metadata')
        metadata = pdf_reader.metadata
        pdf_writer.add_metadata(metadata)
    else:
        logger.debug('Removing old metadata')

    # Add the new metadata
    pdf_writer.add_metadata({metadata_name: metadata_value})

    # Overwrite input pdf
    with open(filename, 'wb') as f:
        pdf_writer.write(f)
        f.close()


def add_png_metadata(png_path: str, metadata: dict, loglevel: str = 'WARNING'):
    """
    Add metadata to a PNG image file.

    Args:
        png_path (str): The path to the PNG image file.
        metadata (dict): A dictionary of metadata to add to the PNG file.
        loglevel (str): The log level. Default is 'WARNING'.
    """
    logger = log_configure(loglevel, 'add_png_metadata')

    image = Image.open(png_path)

    # Create a dictionary for the PNG metadata
    png_info = PngImagePlugin.PngInfo()

    # Add the new metadata
    for key, value in metadata.items():
        png_info.add_text(key, value)
        logger.debug(f'Adding metadata: {key} = {value}')

    # Save the file with the new metadata
    image.save(png_path, "PNG", pnginfo=png_info)
    logger.info(f"Metadata added to PNG: {png_path}")


def open_image(file_path: str, loglevel: str = 'WARNING'):
    """
    Open an image file (PNG or PDF), log its metadata, and display a link to the file in the notebook.

    Args:
        file_path (str): The path to the image file.
        loglevel (str): The log level. Default is 'WARNING'.

    Returns:
        None
    """
    logger = log_configure(loglevel, 'open_image')

    # Ensure the file exists
    if not os.path.isfile(file_path):
        logger.error(f"The file {file_path} does not exist.")
        raise FileNotFoundError(f"The file {file_path} does not exist.")

    # Determine the file extension using endswith()
    if file_path.lower().endswith('.png'):
        # Open the PNG file and read the metadata
        image = Image.open(file_path)
        image_metadata = image.info

        # Log the metadata
        print("PNG Metadata:")
        for key, value in image_metadata.items():
            print(f"{key}: {value}")
    elif file_path.lower().endswith('.pdf'):
        # Open the PDF file and read the metadata
        pdf_reader = PdfReader(file_path)
        metadata = pdf_reader.metadata

        # Log the metadata
        print("PDF Metadata:")
        for key, value in metadata.items():
            print(f"{key}: {value}")
    else:
        logger.error(f"Unsupported file type: {file_path}")
        raise ValueError(f"Unsupported file type: {file_path}")

    # Provide a FileLink to the file
    display(FileLink(file_path))
    logger.info(f"Displayed file link for: {file_path}")


def update_metadata_with_date(metadata: dict = None) -> dict:
    """
    Update the provided metadata dictionary with the current date and time.

    Args:
        metadata (dict, optional): The original metadata dictionary.

    Returns:
        dict: The updated metadata dictionary with the current date and time.
    """
    if metadata is None:
        metadata = {}

    now = datetime.datetime.now()
    date_now = now.strftime("%Y-%m-%d %H:%M:%S")
    metadata['date_saved'] = date_now
    return metadata


def username():
    """
    Retrieves the current user's username from the 'USER' environment variable.
    """
    user = os.getenv('USER')
    if user is None:
        raise EnvironmentError("The 'USER' environment variable is not set.")
    return user


class HiddenPrints:
    # from stackoverflow https://stackoverflow.com/questions/8391411/how-to-block-calls-to-print#:~:text=If%20you%20don't%20want,the%20top%20of%20the%20file. # noqa
    def __enter__(self):
        self._original_stdout = sys.stdout
        sys.stdout = open(os.devnull, 'w')

    def __exit__(self, exc_type, exc_val, exc_tb):
        sys.stdout.close()
        sys.stdout = self._original_stdout

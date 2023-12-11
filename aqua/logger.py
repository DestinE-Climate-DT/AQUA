"""Module to implement logging configurations"""

import logging
import types
import datetime
import xarray as xr


def log_configure(log_level=None, log_name=None):
    """Set up the logging level cleaning previous existing handlers

    Args:
        log_level: a string or an integer according to the logging module
        log_name: a string defining the name of the logger to be configured

    Returns:
        the logger object to be used, possibly in a class
    """

    # this is the default loglevel for the AQUA framework
    if log_name is None:
        logging.warning('You are configuring the root logger, are you sure this is what you want?')

    # get the logger
    logger = logging.getLogger(log_name)

    # fix the log level
    log_level = _check_loglevel(log_level)

    # if our logger is already out there, update the logging level and return
    if logger.handlers:
        if log_level != logging.getLevelName(logger.getEffectiveLevel()):
            logger.setLevel(log_level)
            logger.info('Updating the log_level to %s', log_level)
        return logger

    # avoid duplication/propagation of loggers
    logger.propagate = False

    # cannot use BasicConfig for specific loggers
    logger.setLevel(log_level)

    # create console handler which logs
    terminal = logging.StreamHandler()
    # ch.setLevel(log_level)
    terminal.setFormatter(CustomLogColors()) # use the custom formatter
    logger.addHandler(terminal)

    # this can be used in future to log to file
    # fh = logging.FileHandler('spam.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

    return logger


def _check_loglevel(log_level=None):
    """
    Basic function to check the log level so that it can be used
    in other logging functions

    Args:
        log_level: a string or an integer according to the logging module

    Returns:
        the log level as a string

    Raises:
        ValueError: if the log level is not a string or an integer
    """

    log_level_default = 'WARNING'

    # ensure that loglevel is uppercase if it is a string
    if isinstance(log_level, str):
        log_level = log_level.upper()
    # convert to a string if is an integer
    elif isinstance(log_level, int):
        log_level = logging.getLevelName(log_level)
    # if nobody assigned, set it to none
    elif log_level is None:
        log_level = log_level_default
    # error!
    else:
        raise ValueError('Invalid log level type, must be a string or an integer!')

    # use conversion to integer to check if value exist, set None if unable to do it
    log_level_int = getattr(logging, log_level, None)

    # set up a default
    if log_level_int is None:
        logging.warning("Invalid logging level '%s' specified. Setting it back to default %s", log_level, log_level_default)
        log_level = log_level_default

    return log_level


def log_history_iter(data, msg):
    """Elementary provenance logger in the history attribute also for iterators."""
    if isinstance(data, types.GeneratorType):
        data = _log_history_iter(data, msg)
        return data
    else:
        log_history(data, msg)
        return data


def _log_history_iter(data, msg):
    """Iterator loop convenience function for log_history_iter"""
    for ds in data:
        log_history(ds, msg)
        yield ds


def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, (xr.DataArray, xr.Dataset)):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})


class CustomLogColors(logging.Formatter):
    """small class for setting up personalized colors for logging"""

    GREY = "\x1b[38;20m"
    LGREY = "\x1b[37m"
    # DGREY = "\x1b[90m"
    #  LBLUE = "\x1b[38;2;64;183;197m"
    # GREEN = "\x1b[32m"  # Less vibrant green
    # ORANGE = "\x1b[33m" # Less vibrant orange
    # RED = "\x1b[31;20m"  # Less vibrant red
    GREEN = "\x1b[38;2;64;184;50m"  # Vibrant green
    ORANGE = "\x1b[38;2;255;165;0m"  # Vibrant orange
    RED = "\x1b[38;2;255;0;0m"  # Vibrant red
    BOLD_RED = "\x1b[31;1m"
    RESET = "\x1b[0m"

    FORMATS = {
        logging.DEBUG: f"{LGREY}%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s{RESET}",
        logging.INFO: f"{GREY}%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s{RESET}",
        logging.WARNING: f"{ORANGE}%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s{RESET}",
        logging.ERROR: f"{RED}%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s{RESET}",
        logging.CRITICAL: f"{BOLD_RED}%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s{RESET}"
    }

    def format(self, record):
        log_fmt = self.FORMATS.get(record.levelno)
        datefmt = '%Y-%m-%d %H:%M:%S'
        formatter = logging.Formatter(fmt=log_fmt, datefmt=datefmt)
        return formatter.format(record)


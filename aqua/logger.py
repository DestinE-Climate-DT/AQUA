"""Module to implement logging configurations"""

import logging


def log_configure(log_level=None, log_name=None):
    """Set up the logging level cleaning previous existing handlers

    Args:
        log_level: a string or an integer according to the logging module
        log_name: a string defining the name of the logger to be configured

    Returns:
        the logger object to be used, possibly in a class
    """

    # this is the default loglevel for the AQUA framework
    log_level_default = 'WARNING'
    if log_name is None:
        logging.warning('You are configuring the root logger, are you sure this is what you want?')

    # getting the logger
    logger = logging.getLogger(log_name)

    # ensure that loglevel is uppercase if it is a string
    if isinstance(log_level, str):
        log_level = log_level.upper()
    # convert to a string if is an integer
    elif isinstance(log_level, int):
        log_level = logger.getLevelName(log_level)
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

    # clear the handlers of the possibly previously configured logger
    for handler in logger.handlers[:]:
        logger.removeHandler(handler)

    # cannot use BasicConfig for specific loggers
    logger.setLevel(log_level)

    # create formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s :: %(name)s :: %(levelname)-8s -> %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S')

    # create console handler which logs
    ch = logging.StreamHandler()
    ch.setLevel(log_level)
    ch.setFormatter(formatter)
    logger.addHandler(ch)

    # this can be used in future to log to file
    # fh = logging.FileHandler('spam.log')
    # fh.setLevel(logging.DEBUG)
    # fh.setFormatter(formatter)
    # logger.addHandler(fh)

    return logger

"""
This module provides utilities for working with ecCodes, specifically
to retrieve attributes of GRIB parameters by their short names or param IDs.
It operates with caching to improve performance and handles preferentially GRIB2 format.
A tentative is done to access also GRIB1 format in case of errors with GRIB2, but it 
should be noted that GRIB1 is deprecated and not recommended for use.
"""
#import os
#import eccodes
#from packaging import version

import functools
from eccodes import codes_grib_new_from_samples, codes_set, codes_get, codes_release
from eccodes import CodesInternalError
from aqua.core.logger import log_configure
from aqua.core.exceptions import NoEcCodesShortNameError

# some eccodes shortnames are not unique: we need a manual mapping
#NOT_UNIQUE_SHORTNAMES = {
#    'tcc': [228164, 164]
#}


@functools.cache
def _get_attrs_from_shortname(sn, grib_version="GRIB2", table=0, production_status=None):
    """Get the attributes of a parameter by its short name.
    Args:
        sn (str): The short name to look up.
        grib_version (str): The GRIB version to use, either "GRIB2" or "GRIB1".
        table (int or str): The centre/table to use (0 for WMO, 'ecmf' for ECMWF).
        production_status (int, optional): The productionStatusOfProcessedData value.
            Used for centre-specific definitions (e.g., 12 or 13 for Destine).
    Returns:
        dict: A dictionary containing the attributes of the parameter, namely
        'paramId', 'long_name', 'units', 'shortName', 'cfVarName'.
    """

    gid = codes_grib_new_from_samples(grib_version)
    #if sn in NOT_UNIQUE_SHORTNAMES:
    #    # If the short name is special, we need to handle it differently
    #    # by using the first paramId in the list of not unique ones
    #    pid = NOT_UNIQUE_SHORTNAMES[sn][0]
    #    codes_set(gid, "paramId", pid)
    #else:
    #    codes_set(gid, "shortName", sn)
    #    pid = codes_get(gid, "paramId", ktype=str)

    # setting centre to 0 brings the WMO table on top of everything
    codes_set(gid, "centre", table)

    # Set production status if specified (needed for centre-specific definitions like Destine)
    if production_status is not None:
        codes_set(gid, "productionStatusOfProcessedData", production_status)

    codes_set(gid, "shortName", sn)
    pid = codes_get(gid, "paramId", ktype=str)
    nm = codes_get(gid, "name")
    un = codes_get(gid, "units")
    # cf = codes_get(gid, "cfName")
    cfv = codes_get(gid, "cfVarName")
    codes_release(gid)
    return {
        'paramId': pid,
        'long_name': nm,
        'units': un,
        'shortName': sn,
        #'cfName': cf,
        'cfVarName': cfv
    }


@functools.cache
def _get_shortname_from_paramid(pid, production_status=None):
    """Get the attributes of a parameter by its paramId.

    Args:
        pid (str or int): The parameter ID to look up.
        production_status (int, optional): The productionStatusOfProcessedData value.
            Used for centre-specific definitions (e.g., 12 or 13 for Destine).

    Returns:
        string: The short name associated with the given paramId.
    """
    gid = codes_grib_new_from_samples("GRIB2")

    # Set production status if specified (needed for centre-specific definitions like Destine)
    if production_status is not None:
        codes_set(gid, "productionStatusOfProcessedData", production_status)

    codes_set(gid, "paramId", pid)
    sn = codes_get(gid, "shortName")
    codes_release(gid)
    return sn


def get_eccodes_attr(sn, loglevel='WARNING'):
    """
    Wrapper for _get_attrs_from_shorthName to retrieve attributes for a given short name.
    Args:
        sn (str): The short name to look up.
        loglevel (str): The logging level to use for the logger.
    Returns:
        dict: A dictionary containing the attributes of the parameter.
    Raises:
        NoEcCodesShortNameError: If the short name cannot be found in either GRIB
    """
    logger = log_configure(log_level=loglevel, log_name='eccodes')

    # If sn is an integer or a string that can be converted to an integer, treat it as a paramId
    if isinstance(sn, int) or (isinstance(sn, str) and sn.isdigit()):
        logger.debug('Input is a paramId: %s', sn)
        # Try to get short name with different production status values
        for prod_status in [None, 12, 13]:
            try:
                sn = _get_shortname_from_paramid(sn, production_status=prod_status)
                if prod_status is not None:
                    logger.debug('Successfully resolved paramId %s using production status %s', sn, prod_status)
                break
            except CodesInternalError:
                if prod_status == 13:  # Last attempt failed
                    raise NoEcCodesShortNameError(f"Cannot find any grib codes for paramId {sn}")
                continue
    # extract the short name from the variable name if it starts with 'var'
    elif sn.startswith("var"):
        logger.debug('Input is a variable name, extracting short name from: %s', sn)
        # Try to get short name with different production status values
        for prod_status in [None, 12, 13]:
            try:
                sn = _get_shortname_from_paramid(sn[3:], production_status=prod_status)
                if prod_status is not None:
                    logger.debug('Successfully resolved variable name using production status %s', prod_status)
                break
            except CodesInternalError:
                if prod_status == 13:  # Last attempt failed
                    raise NoEcCodesShortNameError(f"Cannot find any grib codes for variable name {sn}")
                continue

    #warning at wrapper level to avoid duplication of logger
    #if sn in NOT_UNIQUE_SHORTNAMES:
    #    logger.warning('Short name %s is not unique, using the first paramId in the list: %s',
    #                   sn, NOT_UNIQUE_SHORTNAMES[sn][0])

    # Try to get attributes from multiple tables and production status combinations
    # The strategies include:
    # - WMO and ECMWF centres (table 0 and 'ecmf')
    # - GRIB2 and GRIB1 versions
    # - Destine production status (12 for operational, 13 for test)
    strategies = [
        {"grib_version": "GRIB2", "table": 0, "production_status": None},
        {"grib_version": "GRIB2", "table": "ecmf", "production_status": None},
        {"grib_version": "GRIB2", "table": 0, "production_status": 12},  # Destine operational
        {"grib_version": "GRIB2", "table": 0, "production_status": 13},  # Destine test
        {"grib_version": "GRIB1", "table": 0, "production_status": None},
        {"grib_version": "GRIB1", "table": "ecmf", "production_status": None},
    ]

    for _, strategy in enumerate(strategies):
        try:
            logger.debug("Trying short name %s with GRIB version %s, table %s, production status %s",
                         sn, strategy["grib_version"], strategy["table"], strategy["production_status"])
            return _get_attrs_from_shortname(sn, **strategy)
        except CodesInternalError as e:
            if strategy["grib_version"] == "GRIB1":
                logger.warning("No GRIB2 codes found, trying GRIB1 for shortName %s", sn)
            logger.debug("Failed guessing for shortName %s, grib_version %s, table %s, production_status %s: %s",
                         sn, strategy["grib_version"], strategy["table"], strategy["production_status"], e)

    raise NoEcCodesShortNameError(f"Cannot find any grib codes for ShortName {sn}")


def get_eccodes_shortname(pid):
    """
    Wrapper for _get_shortname_from_paramid to retrieve the short name for a given paramId.
    Tries multiple production status values (None, 12, 13) to handle centre-specific definitions.

    Args:
        pid (str or int): The parameter ID to look up.

    Returns:
        str: The short name associated with the given paramId.

    Raises:
        NoEcCodesShortNameError: If the paramId cannot be found with any strategy.
    """
    for prod_status in [None, 12, 13]:
        try:
            return _get_shortname_from_paramid(pid, production_status=prod_status)
        except CodesInternalError:
            if prod_status == 13:  # Last attempt failed
                raise NoEcCodesShortNameError(f"Cannot find any grib codes for paramId {pid}")
            continue

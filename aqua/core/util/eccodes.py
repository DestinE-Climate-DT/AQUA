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
def _get_attrs_from_shortname(sn, grib_version="GRIB2", table=0):
    """Get the attributes of a parameter by its short name.
    Args:
        sn (str): The short name to look up.
        grib_version (str): The GRIB version to use, either "GRIB2" or "GRIB1".
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

    # setting cetre to 0 bring the WMO table on top of everything
    codes_set(gid, "centre", table)
    codes_set(gid, "shortName", sn)
    pid = codes_get(gid, "paramId", ktype=str)
    nm = codes_get(gid, "name")
    un = codes_get(gid, "units")
    #cf = codes_get(gid, "cfName")
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
def _get_shortname_from_paramid(pid):
    """Get the attributes of a parameter by its paramId.

    Args:
        paramid (str): The parameter ID to look up.

    Returns:
        string: The short name associated with the given paramId.
    """
    gid = codes_grib_new_from_samples("GRIB2")
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
        sn = _get_shortname_from_paramid(sn)
    # extract the short name from the variable name if it starts with 'var'
    if sn.startswith("var"):
        logger.debug('Input is a variable name, extracting short name from: %s', sn)
        sn = _get_shortname_from_paramid(sn[3:])

    #warning at wrapper level to avoid duplication of logger
    #if sn in NOT_UNIQUE_SHORTNAMES:
    #    logger.warning('Short name %s is not unique, using the first paramId in the list: %s',
    #                   sn, NOT_UNIQUE_SHORTNAMES[sn][0])

    # Try to get attributes from 4 tables: WMO+GRIB2, ECMF+GRIB2, WMO+GRIB1, ECMF+GRIB1
    strategies = [
        {"grib_version": "GRIB2", "table": 0},
        {"grib_version": "GRIB2", "table": "ecmf"},
        {"grib_version": "GRIB1", "table": 0},
        {"grib_version": "GRIB1", "table": "ecmf"},
    ]

    for _, strategy in enumerate(strategies):
        try:
            logger.debug("Trying short name %s with GRIB version %s and table %s",
             sn, strategy["grib_version"], strategy["table"])
            return _get_attrs_from_shortname(sn, **strategy)
        except CodesInternalError as e:
            if strategy["grib_version"] == "GRIB1":
                logger.warning("No GRIB2 codes found, trying GRIB1 for shortName %s", sn)
            logger.debug("Failed guessing for shortName %s, grib_version %s and table %s: %s",
                         strategy["grib_version"], strategy["table"], sn, e)

    raise NoEcCodesShortNameError(f"Cannot find any grib codes for ShortName {sn}")


def get_eccodes_shortname(pid):
    """
    Wrapper for _get_shortname_from_paramid to retrieve the short name for a given paramId.
    """
    try:
        return _get_shortname_from_paramid(pid)
    except CodesInternalError as e:
        raise NoEcCodesShortNameError(f"Cannot find any grib codes for paramId {pid}") from e

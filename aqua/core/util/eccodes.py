"""
This module provides utilities for working with ecCodes, specifically
to retrieve attributes of GRIB parameters by their short names or param IDs.
It operates with caching to improve performance and handles preferentially GRIB2 format.
A tentative is done to access also GRIB1 format in case of errors with GRIB2, but it
should be noted that GRIB1 is deprecated and not recommended for use.
"""
# import os
# import eccodes
# from packaging import version

import functools

from eccodes import CodesInternalError, codes_get, codes_grib_new_from_samples, codes_release, codes_set

from aqua.core.exceptions import NoEcCodesShortNameError
from aqua.core.logger import log_configure

# some eccodes shortnames are not unique: we need a manual mapping
# NOT_UNIQUE_SHORTNAMES = {
#    'tcc': [228164, 164]
# }


@functools.cache
def _get_attrs_from_shortname_or_pid(sn=None, pid=None, grib_version="GRIB2", table=0, logger=None):
    """Get the attributes of a parameter by its short name or pid.
    Args:
        sn (str): The short name to look up.
        pid (str): The parameter ID to look up.
        grib_version (str): The GRIB version to use, either "GRIB2" or "GRIB1".
        logger (logging.Logger, optional): The logger to use for logging. Defaults to None.
    Returns:
        dict: A dictionary containing the attributes of the parameter, namely
        'paramId', 'long_name', 'units', 'shortName', 'cfVarName'.
    """

    gid = codes_grib_new_from_samples(grib_version)

    # setting cetre to 0 bring the WMO table on top of everything
    codes_set(gid, "centre", table)
    # HACK: if the sn is not defined in the WMO table, first set the GRIB2 template
    # handler to use Destine local parameters definitions (12)

    if sn:
        try:
            codes_set(gid, "shortName", sn)
        except CodesInternalError:
            if logger:
                logger.debug("shortName %s not found in default WMO definitions, switching to DestinE local parameters", sn)
            codes_set(gid, "productionStatusOfProcessedData", 12)
            codes_set(gid, "shortName", sn)
        pid = codes_get(gid, "paramId", ktype=str)
    else:
        try:
            codes_set(gid, "paramId", pid)
        except CodesInternalError:
            if logger:
                logger.debug("paramId %s not found in default WMO definitions, switching to DestinE local parameters", pid)
            codes_set(gid, "productionStatusOfProcessedData", 12)
            codes_set(gid, "paramId", pid)
        sn = codes_get(gid, "shortName")

    nm = codes_get(gid, "name")
    un = codes_get(gid, "units")
    # cf = codes_get(gid, "cfName")
    cfv = codes_get(gid, "cfVarName")
    codes_release(gid)
    return {
        "paramId": pid,
        "long_name": nm,
        "units": un,
        "shortName": sn,
        #'cfName': cf,
        "cfVarName": cfv,
    }


def get_eccodes_attr(sn, loglevel="WARNING"):
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
    logger = log_configure(log_level=loglevel, log_name="eccodes")

    # If sn is an integer or a string that can be converted to an integer, treat it as a paramId
    if isinstance(sn, str) and sn.startswith("var"):
        sn = sn[3:]
    if isinstance(sn, int) or (isinstance(sn, str) and sn.isdigit()):
        pid = str(sn)
        sn = None
    else:
        pid = None

    # Try to get attributes from 4 tables: WMO+GRIB2, ECMF+GRIB2, WMO+GRIB1, ECMF+GRIB1
    strategies = [
        {"grib_version": "GRIB2", "table": 0},
        {"grib_version": "GRIB2", "table": "ecmf"},
        {"grib_version": "GRIB1", "table": 0},
        {"grib_version": "GRIB1", "table": "ecmf"},
    ]

    for _, strategy in enumerate(strategies):
        if sn:
            logger.debug(
                "Trying short name %s with GRIB version %s and table %s", sn, strategy["grib_version"], strategy["table"]
            )
        else:
            logger.debug(
                "Trying paramId %s with GRIB version %s and table %s", pid, strategy["grib_version"], strategy["table"]
            )

        try:
            return _get_attrs_from_shortname_or_pid(sn=sn, pid=pid, **strategy, logger=logger)
        except CodesInternalError as e:
            if strategy["grib_version"] == "GRIB1":
                logger.debug("No GRIB2 codes found, trying GRIB1 for shortName %s", sn)
            if sn:
                logger.debug(
                    "Failed guessing for shortName %s, grib_version %s and table %s: %s",
                    strategy["grib_version"],
                    strategy["table"],
                    sn,
                    e,
                )
            else:
                logger.debug(
                    "Failed guessing for paramId %s, grib_version %s and table %s: %s",
                    strategy["grib_version"],
                    strategy["table"],
                    pid,
                    e,
                )

    if sn:
        raise NoEcCodesShortNameError(f"Cannot find any grib codes for ShortName {sn}")
    else:
        raise NoEcCodesShortNameError(f"Cannot find any grib codes for paramId {pid}")

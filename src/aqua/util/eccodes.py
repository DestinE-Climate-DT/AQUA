"""Utilities for ecCodes"""
import os
import eccodes
from ruamel.yaml import YAML
from aqua.logger import log_configure
from aqua.exceptions import NoEcCodesShortNameError


# Currently not used
# def read_eccodes_dic(filename):
#     """
#     Reads an ecCodes definition file and returns its contents as a dictionary.

#     Parameters:
#     - filename (str): The name of the ecCodes definition file to read.

#     Returns:
#     - A dictionary containing the contents of the ecCodes definition file.
#     """
#     yaml = YAML(typ='rt')
#     fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
#     fn = os.path.join(fn, 'grib2', filename)
#     with open(fn, "r", encoding='utf-8') as file:
#         text = file.read()
#     text = text.replace(" =", ":").replace('{', '').replace('}', '').replace(';', '').replace('\t', '    ')
#     return yaml.load(text)


def read_eccodes_def(filename):
    """
    Reads an ecCodes definition file and returns its keys as a list.

    Parameters:
        filename (str): The name of the ecCodes definition file to read.

    Returns:
        A list containing the keys of the ecCodes definition file.
    """

    # This allows to further extend the list of grib_version and tables to read
    # when looking for eccodes definitions.
    keylist = {'grib2': {
                    'wmo': [],
                    'ecmwf': []
                },
               'grib1': {
                   'ecmwf': []
               }}

    fn_eccodes = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first

    # WMO lists
    fn = os.path.join(fn_eccodes, 'grib2', filename)
    with open(fn, "r", encoding='utf-8') as f:
        for line in f:
            line = line.replace(" =", "").replace('{', '').replace('}', '').replace(';', '').replace('\t', '#    ')
            if not line.startswith("#"):
                keylist['grib2']['wmo'].append(line.strip().replace("'", ""))
    keylist['grib2']['wmo'] = keylist['grib2']['wmo'][:-1]  # The last entry is no good

    # ECMWF lists
    for grib_version in ['grib2', 'grib1']:
        fn_grib = os.path.join(fn_eccodes, grib_version, 'localConcepts', 'ecmf', filename)
        with open(fn_grib, "r", encoding='utf-8') as f:
            for line in f:
                line = line.replace(" =", "").replace('{', '').replace('}', '').replace(';', '').replace('\t', '#    ')
                if not line.startswith("#"):
                    keylist[grib_version]['ecmwf'].append(line.strip().replace("'", ""))
        keylist[grib_version]['ecmwf'] = keylist[grib_version]['ecmwf'][:-1]  # The last entry is no good

    return keylist


# Define this as a closure to avoid reading twice the same file
def _init_get_eccodes_attr():
    shortname = read_eccodes_def("shortName.def")
    paramid = read_eccodes_def("paramId.def")
    name = read_eccodes_def("name.def")
    cfname = read_eccodes_def("cfName.def")
    cfvarname = read_eccodes_def("cfVarName.def")
    units = read_eccodes_def("units.def")

    def _get_eccodes_attr(sn, loglevel='WARNING'):
        """
        Recover eccodes attributes for a given short name

        Args:
            shortname(str): the shortname to search
            loglevel (str): the loggin level
        Returns:
            A dictionary containing param, long_name, units, short_name
        """
        logger = log_configure(log_level=loglevel, log_name='eccodes')
        nonlocal shortname, paramid, name, cfname, cfvarname, units

        for grib_version in shortname.keys():
            for table in shortname[grib_version].keys():
                try:
                    if sn.startswith("var"):
                        i = paramid[grib_version][table].index(sn[3:])
                        # indices = [i for i, x in enumerate(paramid) if x == sn[3:]]
                    else:
                        # i = shortname.index(sn)
                        indices = [i for i, x in enumerate(shortname[grib_version][table]) if x == sn]
                        if len(indices) > 1:
                            logger.warning('ShortName %s has multiple grib codes associated: %s',
                                           sn, [paramid[grib_version][table][i] for i in indices])
                            logger.warning('AQUA will take the first so that %s -> %s, please set up a correct fix if this does not look right',  # noqa E501
                                           sn, paramid[grib_version][table][indices[0]])
                        i = indices[0]

                    dic = {"paramId": paramid[grib_version][table][i],
                           "long_name": name[grib_version][table][i],
                           "units": units[grib_version][table][i],
                           "cfVarName": cfvarname[grib_version][table][i],
                           "shortName": shortname[grib_version][table][i]}

                    if 'grib1' in grib_version:
                        logger.warning(f'Variable {shortname[grib_version][table][i]} is found in grib1 tables, please check if it is correct') # noqa E501
                    elif 'ecmwf' in table:
                        logger.info(f'Variable {shortname[grib_version][table][i]} is found in ECMWF local tables')

                    return dic

                except (ValueError, IndexError):
                    logger.debug('Not found for gribversion %s, table %s: %s', grib_version, table)

        logger.error(f'Cannot find any grib codes for ShortName {sn}, returning empty dictionary')

        return None

    return _get_eccodes_attr


get_eccodes_attr = _init_get_eccodes_attr()


# Define this as a closure to avoid reading twice the same file
def init_get_eccodes_shortname():
    """
    Recover eccodes shorthname from a given paramid

    Args:
        var(str, int): the variable name (a short_name or a paramid)

    Returns:
        A string containing the short_name
    """
    shortname = read_eccodes_def("shortName.def")
    paramid = read_eccodes_def("paramId.def")

    def _get_eccodes_shortname(var):
        """
        Allows to retrieve the shortname from the paramid

        Args:
            var(str, int): the variable name (a short_name or a paramid)

        Returns:
            A string containing the short_name
        """
        nonlocal shortname, paramid

        # If we have a digit we have to search for the shortname
        if str(var).isdigit():
            # We loop over the available tables
            for grib_version in shortname.keys():
                for table in shortname[grib_version].keys():
                    try:
                        i = paramid[grib_version][table].index(str(var))
                        return shortname[grib_version][table][i]
                    except (ValueError, IndexError):
                        # We don't have an error yet unless it's the last table to analyze
                        pass
            # Out of the loop, we have not found anything and we have no left table, error
            raise NoEcCodesShortNameError(f'Cannot find any grib codes for paramid {var}')
        else:  # If we have a string there is no need to search
            return var

    return _get_eccodes_shortname

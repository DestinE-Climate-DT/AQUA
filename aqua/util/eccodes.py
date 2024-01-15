"""Utilities for ecCodes"""
import os
import eccodes
from ruamel.yaml import YAML
from aqua.logger import log_configure
from aqua.exceptions import NoEcCodesShortNameError


# Currently not used
def read_eccodes_dic(filename):
    """
    Reads an ecCodes definition file and returns its contents as a dictionary.

    Parameters:
    - filename (str): The name of the ecCodes definition file to read.

    Returns:
    - A dictionary containing the contents of the ecCodes definition file.
    """
    yaml = YAML(typ='rt')
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2', filename)
    with open(fn, "r", encoding='utf-8') as file:
        text = file.read()
    text = text.replace(" =", ":").replace('{', '').replace('}', '').replace(';', '').replace('\t', '    ')
    return yaml.load(text)


def read_eccodes_def(filename):
    """
    Reads an ecCodes definition file and returns its keys as a list.

    Parameters:
        filename (str): The name of the ecCodes definition file to read.

    Returns:
        A list containing the keys of the ecCodes definition file.
    """

    keylist = []

    # WMO lists
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2', filename)
    with open(fn, "r", encoding='utf-8') as f:
        for line in f:
            line = line.replace(" =", "").replace('{', '').replace('}', '').replace(';', '').replace('\t', '#    ')
            if not line.startswith("#"):
                keylist.append(line.strip().replace("'", ""))
    keylist = keylist[:-1]  # The last entry is no good

    # ECMWF lists
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2', 'localConcepts', 'ecmf', filename)
    with open(fn, "r", encoding='utf-8') as f:
        for line in f:
            line = line.replace(" =", "").replace('{', '').replace('}', '').replace(';', '').replace('\t', '#    ')
            if not line.startswith("#"):
                keylist.append(line.strip().replace("'", ""))

    # The last entry is no good
    return keylist[:-1]


# Define this as a closure to avoid reading twice the same file
def _init_get_eccodes_attr():
    shortname = read_eccodes_def("shortName.def")
    paramid = read_eccodes_def("paramId.def")
    name = read_eccodes_def("name.def")
    cfname = read_eccodes_def("cfName.def")
    cfvarname = read_eccodes_def("cfVarName.def")
    units = read_eccodes_def("units.def")

    def _get_eccodes_attr(sn, loglevel='warning'):
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
        try:
            if sn.startswith("var"):
                i = paramid.index(sn[3:])
                # indices = [i for i, x in enumerate(paramid) if x == sn[3:]]
            else:
                # i = shortname.index(sn)
                indices = [i for i, x in enumerate(shortname) if x == sn]
                if len(indices) > 1:
                    logger.warning('ShortName %s has multiple grib codes associated: %s', sn, [paramid[i] for i in indices])
                    logger.warning('AQUA will take the first so that %s -> %s, please set up a correct fix if this does not look right',  # noqa E501
                                   sn, paramid[indices[0]])
                i = indices[0]

            dic = {"paramId": paramid[i],
                   "long_name": name[i],
                   "units": units[i],
                   "cfVarName": cfvarname[i],
                   "shortName": shortname[i]}
            return dic

        except (ValueError, IndexError) as error:
            logger.debug('ERROR: %s', error)
            logger.warning('Cannot find any grib codes for ShortName %s, returning empty dictionary', sn)
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
        """
        nonlocal shortname, paramid

        if str(var).isdigit():
            try:
                i = paramid.index(str(var))
                return shortname[i]
            except (ValueError, IndexError) as error:
                raise NoEcCodesShortNameError('Cannot find any grib codes for paramId %s' % var) from error
        else:
            return var

    return _get_eccodes_shortname

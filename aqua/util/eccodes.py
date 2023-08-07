"""Utilities for ecCodes"""
import os
import eccodes
from ruamel.yaml import YAML


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
    fn = os.path.join(fn, 'grib2',  'localConcepts', 'ecmf', filename)
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

    def _get_eccodes_attr(sn):
        """
        Recover eccodes attributes for a given short name

        Args:
            shortname(str): the shortname to search
        Returns:
            A dictionary containing param, long_name, units, short_name
        """
        nonlocal shortname, paramid, name, cfname, cfvarname, units
        try:
            if sn.startswith("var"):
                i = paramid.index(sn[3:])
            else:
                i = shortname.index(sn)

            dic = {"paramId": paramid[i],
                   "long_name": name[i],
                   "units": units[i],
                   "cfVarName": cfvarname[i],
                   "shortName": shortname[i]}
            return dic
        except ValueError:
            print(f"Conversion Error: variable '{sn}' not found in ECMWF tables!")
            return

    return _get_eccodes_attr


get_eccodes_attr = _init_get_eccodes_attr()

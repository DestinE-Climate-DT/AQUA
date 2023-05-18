"""Module containing general utility functions for AQUA"""

import datetime
import operator
import os
import random
import re
import string
import sys
import logging
from collections import defaultdict
import yaml
import eccodes
import xarray as xr
import string
import random
import datetime
import types
from aqua.logger import log_configure


def load_yaml(infile):
    """
    Load generic yaml file

    Args:
        infile(str): a file path

    Returns:
        A dictionary with the yaml file keys
    """

    try:
        with open(infile, 'r', encoding='utf-8') as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
    except IOError:
        sys.exit(f'ERROR: {infile} not found: you need to have this configuration file!')
    return cfg


def load_multi_yaml(folder_path):
    """
    Load and merge all yaml files located in a given folder into a single dictionary.

    Args:
        folder_path(str): The path of the folder containing the yaml files to be merged.

    Returns:
        A dictionary containing the merged contents of all the yaml files.
    """

    merged_dict = defaultdict(dict)
    for filename in os.listdir(folder_path):
        if filename.endswith(('.yml', '.yaml')):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r', encoding='utf-8') as file:
                yaml_dict = yaml.safe_load(file)
                for key, value in yaml_dict.items():
                    merged_dict[key].update(value)
    return dict(merged_dict)


def get_config_dir():
    """
    Return the path to the configuration directory,
    searching in a list of pre-defined directories.

     Args:
        None
     Returns:
         configdir (str): the dir of the catalog file and other config files
    """

    # set of predefined folders to browse
    configdirs = ['./config', '../config', '../../config', '../../../config']

    # if the home is defined
    homedir = os.environ.get('HOME')
    if homedir:
        configdirs.append(os.path.join(homedir, '.aqua', 'config'))
    
    # autosearch
    for configdir in configdirs:
        if os.path.exists(os.path.join(configdir, "config.yaml")):
            break
    return configdir


def _eval_formula(mystring, xdataset):
    """Evaluate the cmd string provided by the yaml file
    producing a parsing for the derived variables"""

    # Tokenize the original string
    token = [i for i in re.split('([^\\w.]+)', mystring) if i]
    if len(token) > 1:
        # Special case, start with -
        if token[0] == '-':
            out = -xdataset[token[1]]
        else:
            # Use order of operations
            out = _operation(token, xdataset)
    else:
        out = xdataset[token[0]]
    return out


def _operation(token, xdataset):
    """Parsing of the CDO-based commands using operator package
    and an ad-hoc dictionary. Could be improved, working with four basic
    operations only."""

    # define math operators: order is important, since defines
    # which operation is done at first!
    ops = {
        '/': operator.truediv,
        "*": operator.mul,
        "-": operator.sub,
        "+": operator.add
    }

    # use a dictionary to store xarray field and call them easily
    dct = {}
    for k in token:
        if k not in ops:
            try:
                dct[k] = float(k)
            except ValueError:
                dct[k] = xdataset[k]

    # apply operators to all occurrences, from top priority
    # so far this is not parsing parenthesis
    code = 0
    for p in ops:
        while p in token:
            code += 1
            # print(token)
            x = token.index(p)
            name = 'op' + str(code)
            # replacer = ops.get(p)(dct[token[x - 1]], dct[token[x + 1]])
            # Using apply_ufunc in order not to
            replacer = xr.apply_ufunc(ops.get(p), dct[token[x - 1]], dct[token[x + 1]],
                                      keep_attrs=True, dask='parallelized')
            dct[name] = replacer
            token[x - 1] = name
            del token[x:x + 2]
    return replacer


def get_machine(configdir):
    """
    Extract the name of the machine from the configuration file

    Args:
        configdir(str): the configuration file directory
     Returns:
        The name of the machine read from the configuration file
    """

    config_file = os.path.join(configdir, "config.yaml")
    if os.path.exists(config_file):
        base = load_yaml(os.path.join(configdir, "config.yaml"))
        return base['machine']
    else:
        sys.exit('Cannot find the basic configuration file!')


def get_reader_filenames(configdir, machine):
    """
    Extract the filenames for the reader for catalog, regrid and fixer

    Args:
        configdir(str): the configuration file directory
        machine(str): the machine on which you are running
     Returns:
        Three strings for the path of the catalog, regrid and fixer files
    """

    config_file = os.path.join(configdir, "config.yaml")
    if os.path.exists(config_file):
        base = load_yaml(os.path.join(configdir, "config.yaml"))
        catalog_file = base['reader']['catalog'].format(machine=machine, configdir=configdir)
        if not os.path.exists(catalog_file):
            sys.exit(f'Cannot find catalog file in {catalog_file}')
        regrid_file = base['reader']['regrid'].format(machine=machine, configdir=configdir)
        if not os.path.exists(regrid_file):
            sys.exit(f'Cannot find catalog file in {regrid_file}')
        fixer_folder = base['reader']['fixer'].format(machine=machine, configdir=configdir)
        if not os.path.exists(fixer_folder):
            sys.exit(f'Cannot find catalog file in {fixer_folder}')

    return catalog_file, regrid_file, fixer_folder, config_file


# Currently not used
def read_eccodes_dic(filename):
    """
    Reads an ecCodes definition file and returns its contents as a dictionary.

    Parameters:
    - filename (str): The name of the ecCodes definition file to read.

    Returns:
    - A dictionary containing the contents of the ecCodes definition file.
    """
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2', filename)
    with open(fn, "r", encoding='utf-8') as file:
        text = file.read()
    text = text.replace(" =", ":").replace('{', '').replace('}', '').replace(';', '').replace('\t', '    ')
    return yaml.safe_load(text)


def read_eccodes_def(filename):
    """
    Reads an ecCodes definition file and returns its keys as a list.

    Parameters:
        filename (str): The name of the ecCodes definition file to read.

    Returns:
        A list containing the keys of the ecCodes definition file.
    """

    # ECMWF lists
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2',  'localConcepts', 'ecmf', filename)
    keylist = []
    with open(fn, "r", encoding='utf-8') as f:
        for line in f:
            line = line.replace(" =", "").replace('{', '').replace('}', '').replace(';', '').replace('\t', '#    ')
            if not line.startswith("#"):
                keylist.append(line.strip().replace("'", ""))

    keylist = keylist[:-1]

    # WMO lists
    fn = eccodes.codes_definition_path().split(':')[0]  # LUMI fix, take only first
    fn = os.path.join(fn, 'grib2', filename)
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


def generate_random_string(length):
    """
    Generate a random string of lowercase and uppercase letters and digits
    """

    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string


def log_history_iter(data, msg):
    """Elementary provenance logger in the history attribute also for iterators."""
    if type(data) is types.GeneratorType:
        for ds in data:
            ds = log_history(ds, msg)
            yield ds
    else:
        data = log_history(data, msg)
        return data


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


def create_folder(folder, loglevel=None):
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
        logger.warning('Creating folder %s', folder)
        os.makedirs(folder)
    else:
        logger.warning('Folder %s already exists', folder)


def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""

    if isinstance(data, xr.DataArray) or isinstance(data, xr.Dataset):
        now = datetime.datetime.now()
        date_now = now.strftime("%Y-%m-%d %H:%M:%S")
        hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
        data.attrs.update({"history": hist})


def file_is_complete(filename, logger=logging.getLogger()):

    """Basic check to see if file exists and that includes values which are not NaN
    Return a boolean that can be used as a flag for further operation
    True means that we have to re-do the computation
    A logger can be passed for correct logging properties"""

    if os.path.isfile(filename):
        logger.info('File %s is found...', filename)
        try:
            xfield = xr.open_dataset(filename)
            varname = list(xfield.data_vars)[0]
            if xfield[varname].isnull().all():
            #if xfield[varname].isnull().all(dim=['lon','lat']).all():
                logger.error('File %s is full of NaN! Recomputing...', filename)
                check=True
            else:
                check=False
                logger.info('File %s seems ok!', filename)
        # we have no clue which kind of exception might show up
        except ValueError:
            logger.info('Something wrong with file %s! Recomputing...', filename)
            check= True
    else:
        logger.info('File %s not found...', filename)
        check = True

    return check


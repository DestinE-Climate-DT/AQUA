import sys
import yaml
import os
import sys
import operator
import re
import eccodes
import xarray as xr
import string
import random
import datetime


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
    configdirs = ['./config', '../config', '../../config']
    homedir = os.environ.get('HOME')

    # if the home is defined
    if homedir:
        configdirs.append(os.path.join(homedir, '.aqua', 'config'))
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
            #replacer = ops.get(p)(dct[token[x - 1]], dct[token[x + 1]])
            # Using apply_ufunc in order not to 
            replacer = xr.apply_ufunc(ops.get(p), dct[token[x - 1]], dct[token[x + 1]], keep_attrs=True, dask='parallelized')
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

    basefile = os.path.join(configdir, "config.yaml")
    if os.path.exists(basefile):
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

    basefile = os.path.join(configdir, "config.yaml")
    if os.path.exists(basefile):
        base = load_yaml(os.path.join(configdir, "config.yaml"))
        catalog_file = base['reader']['catalog'].format(machine=machine, configdir=configdir)
        if not os.path.exists(catalog_file):
            sys.exit(f'Cannot find catalog file in {catalog_file}')
        regrid_file = base['reader']['regrid'].format(machine=machine, configdir=configdir)
        if not os.path.exists(regrid_file):
            sys.exit(f'Cannot find catalog file in {regrid_file}')
        fixer_file = base['reader']['fixer'].format(machine=machine, configdir=configdir)
        if not os.path.exists(fixer_file):
            sys.exit(f'Cannot find catalog file in {fixer_file}')

    return catalog_file, regrid_file, fixer_file


# Currently not used
def read_eccodes_dic(filename):
    """
    Reads an ecCodes definition file and returns its contents as a dictionary.

    Parameters:
    - filename (str): The name of the ecCodes definition file to read.

    Returns:
    - A dictionary containing the contents of the ecCodes definition file.
    """

    fn= os.path.join(eccodes.codes_definition_path(), 'grib2', filename)
    with open(fn, "r") as f:
        text = f.read()
    text = text.replace(" =", ":").replace('{','').replace('}','').replace(';','').replace('\t', '    ')
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
    fn= os.path.join(eccodes.codes_definition_path(), 'grib2',  'localConcepts', 'ecmf', filename)
    list = []
    with open(fn, "r") as f:
        for line in f:
            line = line.replace(" =", "").replace('{','').replace('}','').replace(';','').replace('\t', '#    ')
            if not line.startswith("#"):
                list.append(line.strip().replace("'", ""))

    list = list[:-1]
    
    # WMO lists
    fn= os.path.join(eccodes.codes_definition_path(), 'grib2', filename)
    with open(fn, "r") as f:
        for line in f:
            line = line.replace(" =", "").replace('{','').replace('}','').replace(';','').replace('\t', '#    ')
            if not line.startswith("#"):
                list.append(line.strip().replace("'", ""))

    # The last entry is no good
    return list[:-1]


# Define this as a closure to avoid reading twice the same file
def _init_get_eccodes_attr():
    shortname = read_eccodes_def("shortName.def")
    paramid = read_eccodes_def("paramId.def")
    name = read_eccodes_def("name.def")
    cfname = read_eccodes_def("cfName.def")
    cfvarname = read_eccodes_def("cfVarName.def")
    units = read_eccodes_def("units.def")

    def get_eccodes_attr(sn):
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
                i =  paramid.index(sn[3:])
            else:
                i =  shortname.index(sn)
                
            dic = {"paramId": paramid[i],
                "long_name": name[i],
                "units": units[i],
                "cfVarName": cfvarname[i],
                "shortName": shortname[i]}
            return dic
        except ValueError:
            print(f"Conversion Error: variable '{sn}' not found in ECMWF tables!")
            return

    return get_eccodes_attr

get_eccodes_attr = _init_get_eccodes_attr()


def generate_random_string(length):
    """
    Generate a random string of lowercase and uppercase letters and digits
    """
   
    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string


def log_history(data, msg):
    """Elementary provenance logger in the history attribute"""
    
    now = datetime.datetime.now()
    date_now = now.strftime("%Y-%m-%d %H:%M:%S")
    hist = data.attrs.get("history", "") + f"{date_now} {msg};\n"
    data.attrs.update({"history": hist})
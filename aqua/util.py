import sys
import yaml
import os
import sys
import string
import random

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


def generate_random_string(length):
    """G
    Generate a random string of lowercase and uppercase letters and digits
    """
   
    letters_and_digits = string.ascii_letters + string.digits
    random_string = ''.join(random.choice(letters_and_digits) for _ in range(length))
    return random_string

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

def create_folder(folder, verbose=False):
    """
    Create a folder if it does not exist

    Args:
        folder (str): the folder to create
        verbose (bool): if True, print the folder name,
                        default is False

    Returns:
        None
    """

    if not os.path.exists(folder):
        if verbose:
            print(f'Creating folder {folder}')
        os.makedirs(folder)
    else:
        if verbose:
            print(f'Folder {folder} already exists')
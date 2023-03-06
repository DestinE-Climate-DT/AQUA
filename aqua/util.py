import sys
import yaml
import os
import sys

def load_yaml(infile):
    """Load generic yaml file"""

    try:
        with open(infile, 'r', encoding='utf-8') as file:
            cfg = yaml.load(file, Loader=yaml.FullLoader)
    except IOError:
        sys.exit(f'ERROR: {infile} not found: you need to have this configuration file!')
    return cfg

def get_catalog_file(configdir=None):
    """
    Return the path to the configdir and to the catalogfile catalog file, 
    searching in a list of pre-defined directories. If configdir is provided, search only in that directory.
    
    Args:
        configdir (str, optional): Where to search the catalog  and other config files

    Returns:
        configdir (str): the dir of the catalog file and other config files
        catalog_file (str): the path of the catalog file
    
    """

    # if the configdir path is provided
    if configdir:
        catalog_file = os.path.join(configdir, "catalog.yaml")
    else:
        # set of predefined folders to browse
        configdirs = ['./config', '../config', '../../config']
        homedir = os.environ.get('HOME')
        # if the home is defined
        if homedir:
            configdirs.append(os.path.join(homedir, '.aqua', 'config'))
        for configdir in configdirs:
            catalog_file = os.path.join(configdir, "catalog.yaml")
            if os.path.exists(catalog_file):
                break
    
    # safety check to verify existence of the catalogfile
    if os.path.exists(catalog_file):
        return configdir, catalog_file
    else:
        sys.exit('Cannot find the catalog file!')
        
"""YAML utility functions"""

import operator
import os
import re
from string import Template
import xarray as xr
from collections import defaultdict
from ruamel.yaml import YAML
from aqua.logger import log_configure

import yaml  # This is needed to allow YAML override in intake


def construct_yaml_merge(loader, node):
    """
    This function is used to enable override in yaml for intake
    """
    if isinstance(node, yaml.ScalarNode):
        # Handle scalar nodes
        return loader.construct_scalar(node)
    else:
        # Handle sequence nodes
        maps = []
        for subnode in node.value:
            maps.append(loader.construct_object(subnode))
        result = {}
        for dictionary in reversed(maps):
            result.update(dictionary)
        return result


# Run this to enable YAML override for the yaml package when using SafeLoader in intake 
yaml.SafeLoader.add_constructor(
            'tag:yaml.org,2002:merge',
            construct_yaml_merge)


def load_multi_yaml(folder_path=None, filenames=None,
                    definitions=None, **kwargs):
    """
    Load and merge yaml files.
    If a filenames list of strings is provided, only the yaml files with
    the matching full path will be merged.
    If a folder_path is provided, all the yaml files in the folder will be merged.

    Args:
        folder_path (str, optional): the path of the folder containing the yaml
                                        files to be merged.
        filenames (list, optional): the list of the yaml files to be merged.
        definitions (str or dict, optional): name of the section containing string template
                                                definitions or a dictionary with the same

    Keyword Args:
        loglevel (str, optional): the loglevel to be used, default is 'WARNING'

    Returns:
        A dictionary containing the merged contents of all the yaml files.
    """
    yaml = YAML()  # default, if not specified, is 'rt' (round-trip) # noqa F841

    if isinstance(definitions, str):  # if definitions is a string we need to read twice
        yaml_dict = _load_merge(folder_path=folder_path, definitions=None,
                                filenames=filenames, **kwargs)  # Read without definitions
        definitions = yaml_dict.get(definitions)
        yaml_dict = _load_merge(folder_path=folder_path, definitions=definitions,
                                filenames=filenames, **kwargs)  # Read again with definitions
    else:  # if a dictionary or None has been passed for definitions we read only once
        yaml_dict = _load_merge(folder_path=folder_path, definitions=definitions,
                                filenames=filenames, **kwargs)

    return yaml_dict


def load_yaml(infile, definitions=None):
    """
    Load yaml file with template substitution

    Args:
        infile (str): a file path to the yaml
        definitions (str or dict, optional): name of the section containing string template
                                             definitions or a dictionary with the same content
    Returns:
        A dictionary with the yaml file keys
    """

    if not os.path.exists(infile):
        raise ValueError(f'ERROR: {infile} not found: you need to have this configuration file!')

    yaml = YAML(typ='rt')  # default, if not specified, is 'rt' (round-trip)

    cfg = None
    # Load the YAML file as a text string
    with open(infile, 'r', encoding='utf-8') as file:
        yaml_text = file.read()

    if isinstance(definitions, str):  # if it is a string extract from original yaml, else it is directly a dict
        cfg = yaml.load(yaml_text)
        definitions = cfg.get(definitions)

    if definitions:
        # perform template substitution
        template = Template(yaml_text).safe_substitute(definitions)
        cfg = yaml.load(template)
    else:
        if not cfg:  # did we already load it ?
            cfg = yaml.load(yaml_text)

    return cfg


def dump_yaml(outfile=None, cfg=None, typ='rt'):
    """
    Dump to a custom yaml file

    Args:
        outfile(str):   a file path
        cfg(dict):      a dictionary to be dumped
        typ(str):       the type of YAML initialisation.
                        Default is 'rt' (round-trip)
    """
    # Initialize YAML object
    yaml = YAML(typ=typ)

    # Check input
    if outfile is None:
        raise ValueError('ERROR: outfile not defined')
    if cfg is None:
        raise ValueError('ERROR: cfg not defined')

    # Dump the dictionary
    with open(outfile, 'w', encoding='utf-8') as file:
        yaml.dump(cfg, file)


def eval_formula(mystring, xdataset):
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


def _load_merge(folder_path=None, filenames=None,
                definitions=None, merged_dict=None,
                loglevel='WARNING'):
    """
    Helper function for load_merge_yaml.
    Load and merge yaml files located in a given folder
    or a list of yaml files into a dictionary.

    Args:
        folder_path (str, optional):         the path of the folder containing the yaml
                                             files to be merged.
        filenames (list, optional):          the list of the yaml files to be merged.
        definitions (str or dict, optional): name of the section containing string template
                                             definitions or a dictionary with the same content
        merged_dict (dict, optional):        the dictionary to be updated with the yaml files
        loglevel (str, optional):            the loglevel to be used, default is 'WARNING'

    Returns:
        A dictionary containing the merged contents of all the yaml files.

    Raises:
        ValueError: if both folder_path and filenames are None or if both are not None.
    """
    logger = log_configure(log_name='yaml', log_level=loglevel)

    if merged_dict is None:
        logger.debug('Creating a new dictionary')
        merged_dict = defaultdict(dict)
    else:
        logger.debug('Updating an existing dictionary')

    if filenames and folder_path is not None or filenames is None and folder_path is None:
        raise ValueError('ERROR: either folder_path or filenames must be provided')

    if filenames:  # Merging a list of files
        logger.debug(f'Files to be merged: {filenames}')
        for filename in filenames:
            yaml_dict = load_yaml(filename, definitions)
            for key, value in yaml_dict.items():
                merged_dict[key].update(value)

    if folder_path:  # Merging all the files in a folder
        logger.debug(f'Folder to be merged: {folder_path}')
        if not os.path.exists(folder_path):
            raise ValueError(f'ERROR: {folder_path} not found: it is required to have this folder!')
        else:  # folder exists
            for filename in os.listdir(folder_path):
                if filename.endswith(('.yml', '.yaml')):
                    file_path = os.path.join(folder_path, filename)
                    yaml_dict = load_yaml(file_path, definitions)
                    for key, value in yaml_dict.items():
                        merged_dict[key].update(value)

    logger.debug('Dictionary updated')
    logger.debug(f'Keys: {merged_dict.keys()}')

    return merged_dict

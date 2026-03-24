"""YAML utility functions"""

import os
from string import Template as DefaultTemplate
from collections import defaultdict
from tempfile import TemporaryDirectory
from jinja2 import Template
from ruamel.yaml import YAML
import yaml  # This is needed to allow YAML override in intake
from aqua.core.logger import log_configure

def construct_yaml_merge(loader, node):
    """
    This function is used to enable override in yaml for intake
    """
    if isinstance(node, yaml.ScalarNode):
        # Handle scalar nodes
        return loader.construct_scalar(node)
    
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


def load_multi_yaml(folder_path: str | None = None, filenames: list | None = None,
                    definitions: str | dict | None = None, **kwargs):
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


def load_yaml(infile: str, definitions: str | dict | None = None, jinja: bool = True):
    """
    Load yaml file with template substitution

    Args:
        infile (str): a file path to the yaml
        definitions (str or dict, optional): name of the section containing string template
                                             definitions or a dictionary with the same content
        jinja: (bool): jinja2 templating is used instead of standard python templating. Default is true.
    Returns:
        A dictionary with the yaml file keys
    """

    if not os.path.exists(infile):
        raise FileNotFoundError(f'ERROR: {infile} not found: you need to have this configuration file!')

    yaml = YAML(typ='rt')  # default, if not specified, is 'rt' (round-trip)

    cfg = None
    # Load the YAML file as a text string
    with open(infile, 'r', encoding='utf-8') as file:
        yaml_text = file.read()

    if isinstance(definitions, str):  # if it is a string extract from original yaml, else it is directly a dict
        cfg = yaml.load(yaml_text)
        definitions = cfg.get(definitions)

    if definitions:
        # perform template substitution with jinja
        if jinja:
            template = Template(yaml_text)
            rendered_yaml = template.render(definitions)
            cfg = yaml.load(rendered_yaml)
        # use default python templating
        else:
            template = DefaultTemplate(yaml_text).safe_substitute(definitions)
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

    yaml.representer.add_representer(
        type(None),
        lambda self, _: self.represent_scalar('tag:yaml.org,2002:null', 'null')
    )

    # Check input
    if outfile is None:
        raise ValueError('ERROR: outfile not defined')
    if cfg is None:
        raise ValueError('ERROR: cfg not defined')

    # Dump the dictionary with a safe temporary directory
    # to avoid intake reading a partially written file

    # Ensure parent directory exists
    dest_dir = os.path.dirname(os.path.abspath(outfile))
    if dest_dir:  # Handle edge case where filename has no directory component
        os.makedirs(dest_dir, exist_ok=True)
    else:
        dest_dir = '.'  # Use current directory
    with TemporaryDirectory(dir=dest_dir) as tmpdirname:
        tmp_file = os.path.join(tmpdirname, "temp.yaml")
        with open(tmp_file, 'w', encoding='utf-8') as file:
            yaml.dump(cfg, file)
        os.replace(tmp_file, outfile)


def _load_merge(folder_path: str | None = None, filenames: list | None = None,
                definitions: str | dict | None = None, merged_dict: dict | None = None,
                loglevel: str = 'WARNING'):
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

    if filenames is None and folder_path is None:
        raise ValueError('ERROR: at least one between folder_path or filenames must be provided')

    if filenames:  # Merging a list of files
        logger.debug(f'Files to be merged: {filenames}')
        for filename in filenames:
            yaml_dict = load_yaml(filename, definitions)
            for key, value in yaml_dict.items():
                merged_dict[key].update(value)

    if folder_path:  # Merging all the files in a folder
        logger.debug(f'Folder to be merged: {folder_path}')
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f'ERROR: {folder_path} not found: it is required to have this folder!')
        for filename in os.listdir(folder_path):
            if filename.endswith(('.yml', '.yaml')):
                file_path = os.path.join(folder_path, filename)
                yaml_dict = load_yaml(file_path, definitions)
                for key, value in yaml_dict.items():
                    merged_dict[key].update(value)

    logger.debug('Dictionary updated')
    logger.debug(f'Keys: {merged_dict.keys()}')

    return merged_dict

"""YAML utility functions"""

import os
from collections import defaultdict
from string import Template as DefaultTemplate
from tempfile import TemporaryDirectory

import yaml  # This is needed to allow YAML override in intake
from jinja2 import Environment, StrictUndefined
from ruamel.yaml import YAML

from aqua.core.logger import log_configure


def _create_jinja_env(strict: bool = False, preserve_formatting: bool = False):
    """Create a Jinja2 Environment with the given settings.

    Args:
        strict: If True, raise UndefinedError on missing template variables.
                Default False (silently renders as empty string).
        preserve_formatting: If True, preserve template formatting (no trim/lstrip).
                            Default False (trim and lstrip for cleaner YAML).

    Returns:
        A configured Jinja2 Environment.
    """
    kwargs = {
        "trim_blocks": not preserve_formatting,
        "lstrip_blocks": not preserve_formatting,
        "keep_trailing_newline": True,
    }
    if strict:
        kwargs["undefined"] = StrictUndefined
    return Environment(**kwargs)


# Pre-create common Jinja2 environments for efficiency
_JINJA_ENV_BASE = _create_jinja_env()  # Default: trim blocks, soft undefined
_JINJA_ENV_STRICT = _create_jinja_env(strict=True)  # Strict: raise on missing vars
_JINJA_ENV_CATGEN = _create_jinja_env(preserve_formatting=True)  # Preserve: for catalog_entry.j2


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
yaml.SafeLoader.add_constructor("tag:yaml.org,2002:merge", construct_yaml_merge)


def load_multi_yaml(
    folder_path: str | None = None, filenames: list | None = None, definitions: str | dict | None = None, **kwargs
):
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
        yaml_dict = _load_merge(
            folder_path=folder_path, definitions=None, filenames=filenames, **kwargs
        )  # Read without definitions
        definitions = yaml_dict.get(definitions)
        yaml_dict = _load_merge(
            folder_path=folder_path, definitions=definitions, filenames=filenames, **kwargs
        )  # Read again with definitions
    else:  # if a dictionary or None has been passed for definitions we read only once
        yaml_dict = _load_merge(folder_path=folder_path, definitions=definitions, filenames=filenames, **kwargs)

    return yaml_dict


def load_yaml(
    infile: str, definitions: str | dict | None = None, jinja: bool = True, strict: bool = False, catgen: bool = False
):
    """
    Load yaml file with template substitution

    Args:
        infile (str): a file path to the yaml
        definitions (str or dict, optional): name of the section containing string template
                                             definitions or a dictionary with the same content
        jinja (bool): jinja2 templating is used instead of standard python templating. Default is True.
        strict (bool): if True, raises UndefinedError on missing template variables instead of
                       silently rendering them as empty strings. Default is False.
        catgen (bool): if True, use the Jinja environment configured to preserve formatting.

    Returns:
        A dictionary with the yaml file keys
    """

    if not os.path.exists(infile):
        raise FileNotFoundError(f"ERROR: {infile} not found: you need to have this configuration file!")

    yaml = YAML(typ="rt")  # default, if not specified, is 'rt' (round-trip)

    cfg = None
    # Load the YAML file as a text string
    with open(infile, "r", encoding="utf-8") as file:
        yaml_text = file.read()

    # if it is a string extract from original yaml, else it is directly a dict
    if isinstance(definitions, str):
        cfg = yaml.load(yaml_text)
        definitions = cfg.get(definitions)

    if definitions:
        # perform template substitution with jinja
        if jinja:
            if strict:
                template = _JINJA_ENV_STRICT.from_string(yaml_text)
            elif catgen:
                template = _JINJA_ENV_CATGEN.from_string(yaml_text)
            else:
                template = _JINJA_ENV_BASE.from_string(yaml_text)
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


def dump_yaml(outfile=None, cfg=None, typ="rt"):
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

    yaml.representer.add_representer(type(None), lambda self, _: self.represent_scalar("tag:yaml.org,2002:null", "null"))

    # Check input
    if outfile is None:
        raise ValueError("ERROR: outfile not defined")
    if cfg is None:
        raise ValueError("ERROR: cfg not defined")

    # Dump the dictionary with a safe temporary directory
    # to avoid intake reading a partially written file

    # Ensure parent directory exists
    dest_dir = os.path.dirname(os.path.abspath(outfile))
    if dest_dir:  # Handle edge case where filename has no directory component
        os.makedirs(dest_dir, exist_ok=True)
    else:
        dest_dir = "."  # Use current directory
    with TemporaryDirectory(dir=dest_dir) as tmpdirname:
        tmp_file = os.path.join(tmpdirname, "temp.yaml")
        with open(tmp_file, "w", encoding="utf-8") as file:
            yaml.dump(cfg, file)
        os.replace(tmp_file, outfile)


def _load_merge(
    folder_path: str | None = None,
    filenames: list | None = None,
    definitions: str | dict | None = None,
    merged_dict: dict | None = None,
    loglevel: str = "WARNING",
):
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
    logger = log_configure(log_name="yaml", log_level=loglevel)

    if merged_dict is None:
        logger.debug("Creating a new dictionary")
        merged_dict = defaultdict(dict)
    else:
        logger.debug("Updating an existing dictionary")

    if filenames is None and folder_path is None:
        raise ValueError("ERROR: at least one between folder_path or filenames must be provided")

    if filenames:  # Merging a list of files
        logger.debug("Files to be merged: %s", filenames)
        for filename in filenames:
            yaml_dict = load_yaml(filename, definitions)
            for key, value in yaml_dict.items():
                merged_dict[key].update(value)

    if folder_path:  # Merging all the files in a folder
        logger.debug("Folder to be merged: %s", folder_path)
        if not os.path.exists(folder_path):
            raise FileNotFoundError(f"ERROR: {folder_path} not found: it is required to have this folder!")
        for filename in os.listdir(folder_path):
            if filename.endswith((".yml", ".yaml")):
                file_path = os.path.join(folder_path, filename)
                yaml_dict = load_yaml(file_path, definitions)
                for key, value in yaml_dict.items():
                    merged_dict[key].update(value)

    logger.debug("Dictionary updated")
    logger.debug("Keys: %s", merged_dict.keys())

    return merged_dict

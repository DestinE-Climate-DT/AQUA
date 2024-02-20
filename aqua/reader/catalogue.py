"""Simple catalogue utility"""

import intake
from aqua.util import ConfigPath

def catalogue(verbose=True, configdir=None):
    """
    Catalogue of available data.

    Args:
        verbose (bool, optional):       If True, prints the catalog
                                        information to the console.
                                        Defaults to True.
        configdir (str, optional):      The directory containing the
                                        configuration files.
                                        If not provided, get_config_dir
                                        is used to find it.

    Returns:
        cat (intake.catalog.local.LocalCatalog):    The catalog object
                                                    containing the data.
    """

    # get the config dir and the machine
    Configurer = ConfigPath(configdir=configdir)

    # get configuration from the machine
    catalog_file, _, _ = Configurer.get_reader_filenames()

    cat = intake.open_catalog(catalog_file)
    if verbose:
        for model, vm in cat.items():
            for exp, _ in vm.items():
                print(model + '\t' + exp + '\t' + cat[model][exp].description)
                if exp != "grids":
                    for k in cat[model][exp]:
                        print('\t' + '- ' + k + '\t' + cat[model][exp].walk()[k]._description)  # pylint: disable=W0212
            print()
    return cat


def inspect_catalogue(cat=None, model=None, exp=None, source=None, verbose=True):
    """
    Basic function to simplify catalog inspection.
    If a partial match between model, exp and source is provided, then it will return a list
    of models, experiments or possible sources. If all three are specified it returns False if that
    combination does not exist, a list of variables if the source is a FDB/GSV source and it exists and
    True if it exists but is not a FDB source.

    Args:
        cat (intake.catalog.local.LocalCatalog, optional): The catalog object containing the data.
        model (str, optional): The model ID to filter the catalog.
            If None, all models are returned. Defaults to None.
        exp (str, optional): The experiment ID to filter the catalog.
            If None, all experiments are returned. Defaults to None.
        source (str, optional): The source ID to filter the catalog.
            If None, all sources are returned. Defaults to None.
        verbose (bool, optional): Print the catalog information to the console. Defaults to True.

    Returns:
        list:   A list of available items in the catalog, depending on the
                specified model and/or experiment, a list of variables or True/False.

    Raises:
        KeyError: If the input specifications are incorrect.
    """

    if cat is None:
        cat = catalogue(verbose=False)

    if model and exp and not source:
        if is_in_cat(cat, model, exp, None):
            if verbose:
                print(f"Sources available in catalogue for model {model} and exp {exp}:")
            return list(cat[model][exp].keys())
    elif model and not exp:
        if is_in_cat(cat, model, None, None):
            if verbose:
                print(f"Experiments available in catalogue for model {model}:")
            return list(cat[model].keys())
    elif not model:
        if verbose:
            print("Models available in catalogue:")
        return list(cat.keys())

    elif model and exp and source:
        # Check if variables can be explored
        # Added a try/except to avoid the KeyError when the source is not in the catalogue
        # because model or exp are not in the catalogue
        # This allows to always have a True/False or var list return
        # when model/exp/source are provided
        try:
            if is_in_cat(cat, model, exp, source):
                # Ok, it exists, but does it have metadata?
                try:
                    vars = cat[model][exp][source].metadata['variables']
                    if verbose:
                        print(f"The following variables are available for model {model}, exp {exp}, source {source}:")
                    return vars
                except KeyError:
                    return True
        except KeyError:
            pass  # go to return False

    if verbose:
        print(f"The combination model={model}, exp={exp}, source={source} is not available in the catalogue.")
        if model:
            if is_in_cat(cat, model, None, None):
                if exp:
                    if is_in_cat(cat, model, exp, None):
                        print(f"Available sources for model {model} and exp {exp}:")
                        return list(cat[model][exp].keys())
                    else:
                        print(f"Experiment {exp} is not available for model {model}.")
                        print(f"Available experiments for model {model}:")
                        return list(cat[model].keys())
                else:
                    print(f"Available experiments for model {model}:")
                    return list(cat[model].keys())
            else:
                print(f"Model {model} is not available.")
                print("Available models:")
                return list(cat.keys())

    return False


def is_in_cat(cat, model, exp, source):
    """
    Check if the model, experiment and source are in the catalog.
    """
    if source:
        try:
            return source in cat[model][exp].keys()
        except KeyError:
            return False
    elif exp:
        try:
            return exp in cat[model].keys()
        except KeyError:
            return False
    else:
        try:
            return model in cat.keys()
        except KeyError:
            return False

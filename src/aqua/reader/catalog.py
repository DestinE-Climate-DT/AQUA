"""Simple catalog utility"""

import intake
from aqua.util import ConfigPath

def catalog(verbose=True, configdir=None, catalog_name=None):
    """
    Catalog of available data.

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

    # get the config dir and the catalog
    Configurer = ConfigPath(configdir=configdir, catalog=catalog_name)

    # get configuration from the catalog
    catalog_file, _ = Configurer.get_catalog_filenames()

    cat = intake.open_catalog(catalog_file)
    if verbose:
        print('Catalog: ' + Configurer.catalog)
        for model, vm in cat.items():
            for exp, _ in vm.items():
                print(model + '\t' + exp + '\t' + cat[model][exp].description)
                if exp != "grids":
                    for k in cat[model][exp]:
                        print('\t' + '- ' + k + '\t' + cat[model][exp].walk()[k]._description)  # pylint: disable=W0212
            print()
    return cat

def inspect_catalog(catalog_name=None, model=None, exp=None, source=None, verbose=True):
    """
    Basic function to simplify catalog inspection.
    If a partial match between model, exp and source is provided, then it will return a list
    of models, experiments or possible sources. If all three are specified it returns False if that
    combination does not exist, a list of variables if the source is a FDB/GSV source and it exists and
    True if it exists but is not a FDB source.

    Args:
        catalog_name(str, optional): A string containing the catalog name.
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
    
    

    if catalog_name is not None:
        catalog_names = [catalog_name]
    else: # we browse the list of catalogs in the config directory
        catalog_names = ConfigPath().catalog_available

    for cat_name in catalog_names:
        if verbose:
            print(f"Catalog: {cat_name}")
        cat = catalog(catalog_name=cat_name, verbose=False)

        if model and exp and not source:
            if is_in_cat(cat, model, exp, None):
                if verbose:
                    print(f"Sources available in catalog {cat_name} for model {model} and exp {exp}:")
                    print(list(cat[model][exp].keys()))
                # return list(cat[model][exp].keys())
        elif model and not exp:
            if is_in_cat(cat, model, None, None):
                if verbose:
                    print(f"Experiments available in catalog {cat_name} for model {model}:")
                    print(list(cat[model].keys()))
                # return list(cat[model].keys())
        elif not model:
            if verbose:
                print(f"Models available in catalog {cat_name}:")
                print(list(cat.keys()))
            # return list(cat.keys())

        elif model and exp and source:
            # Check if variables can be explored
            # Added a try/except to avoid the KeyError when the source is not in the catalog
            # because model or exp are not in the catalog
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
            print(f"The combination model={model}, exp={exp}, source={source} is not available in the catalog {cat_name}.")
            if model:
                if is_in_cat(cat, model, None, None):
                    if exp:
                        if is_in_cat(cat, model, exp, None):
                            print(f"Available sources for model {model} and exp {exp} in catalog {cat_name}:")
                            print(list(cat[model][exp].keys()))
                            # return list(cat[model][exp].keys())
                        else:
                            print(f"Experiment {exp} is not available for model {model}.")
                            print(f"Available experiments for model {model} in catalog {cat_name}:")
                            print(list(cat[model].keys()))
                            # return list(cat[model].keys())
                    else:
                        print(f"Available experiments for model {model}:")
                        print(list(cat[model].keys()))
                        # return list(cat[model].keys())
                else:
                    print(f"Model {model} is not available.")
                    print("Available models:")
                    print(list(cat.keys()))
                    # return list(cat.keys())

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
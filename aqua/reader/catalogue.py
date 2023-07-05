"""Simple catalogue utility"""

import intake
from aqua.util import get_config_dir, get_machine, get_reader_filenames


def catalogue(verbose=True, configdir=None):
    """Catalogue of available data.

    Args:
        verbose (bool, optional): If True, prints the catalog information to the console. Defaults to True.
        configdir (str, optional): The directory containing the configuration files. If not provided,
            the default configuration directory is used.

    Returns:
        cat (intake.catalog.local.LocalCatalog): The catalog object containing the NextGEMS data.

    """

    # get the config dir and the machine
    if not configdir:
        configdir = get_config_dir()
    machine = get_machine(configdir)

    # get configuration from the machine
    catalog_file, _, _, _ = get_reader_filenames(configdir, machine)

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


def inspect_catalogue(cat, model=None, exp=None):
    """
    Basic function to simplify catalog inspection.

    Args:
        cat (intake.catalog.local.LocalCatalog): The catalog object containing the data.
        model (str, optional): The model ID to filter the catalog.
            If None, all models are returned. Defaults to None.
        exp (str, optional): The experiment ID to filter the catalog.
            If None, all experiments are returned. Defaults to None.

    Returns:
        list: A list of available items in the catalog, depending on the specified model and/or experiment.

    Raises:
        KeyError: If the input specifications are incorrect.
    """

    if model and exp:
        print(f"Sources available in catalogue for model {model} and exp {exp}:")
        return list(cat[model][exp].keys())
    if model and exp is None:
        print(f"Experiments available in catalogue for model {model}:")
        return list(cat[model].keys())
    if model is None and exp is None:
        print("Models available in catalogue:")
        return list(cat.keys())
    raise KeyError("Wrong specifications, cannot inspect the catalog...")

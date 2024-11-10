"""Simple catalog utility"""

import intake
from aqua.util import ConfigPath
from aqua.util.config import scan_catalog


def aqua_catalog(verbose=True, configdir=None, catalog=None):
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
        catalog (str, optional):        Specify a single catalog and avoid 
                                        browsing all available catalogs

    Returns:
        cat (intake.catalog.local.LocalCatalog):    The catalog object
                                                    containing the data.
    """

    # get the config dir and the catalog
    configurer = ConfigPath(configdir=configdir, catalog=catalog)
    
    aquacats = {}
    for aquacat in configurer.catalog_available:

        catalog_file, _ = configurer.get_catalog_filenames(catalog=aquacat)
        cat = intake.open_catalog(catalog_file)
        if verbose:
            print('Catalog: ' + aquacat)
            for model, vm in cat.items():
                for exp, _ in vm.items():
                    print(model + '\t' + exp + '\t' + cat[model][exp].description)
                    if exp != "grids":
                        for k in cat[model][exp]:
                            print('\t' + '- ' + k + '\t' + cat[model][exp].walk()[k]._description)  # pylint: disable=W0212
                print()
        aquacats[aquacat] = cat
    return aquacats


def inspect_catalog(catalog=None, model=None, exp=None, source=None, verbose=True):
    """
    Basic function to simplify catalog inspection.
    If a partial match between model, exp and source is provided, then it will return a list
    of models, experiments or possible sources. If all three are specified it returns False if that
    combination does not exist, a list of variables if the source is a FDB/GSV source and it exists and
    True if it exists but is not a FDB source.

    Args:
        catalog(str, optional): A string containing the catalog name.
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

    aquacats = aqua_catalog(catalog=catalog, verbose=False) 

    # get all info from with the scan_catalog function
    infodict = {}
    for aquacat, cat in aquacats.items():
        infodict[aquacat] = {}
        infodict[aquacat]['check'], infodict[aquacat]['level'], infodict[aquacat]['avail'] = scan_catalog(cat, model, exp, source)

    # return information to the user
    for level in ['variables', 'source', 'exp', 'model']:
        status = find_string_in_dict(infodict, level)
        if status:
            if len(status)>1:
                print("WARNING: inspect_catalog found multiple entries for the required keys!")
                print('Return a dictionary instead of a list!')
                return {key: value['avail'] for key, value in infodict.items() if 'avail' in value}
            printcat = status[0][0]
            if verbose:
                if not model:
                    print(f"Models available in catalog {printcat}:")
                if model and not exp:
                    print(f"Experiments available in catalog {printcat} for model {model}:")
                if model and exp and not source:
                    print(f"Sources available in catalog {printcat} for model {model} and exp {exp}:")
            
            return infodict[status[0][0]]['avail']
    

def find_string_in_dict(data, target_string):
    """Helper function"""
    matches = []
    
    for key, sub_dict in data.items():
        
        if sub_dict['level'] == target_string:
            matches.append((key, sub_dict['level']))
    
    return matches

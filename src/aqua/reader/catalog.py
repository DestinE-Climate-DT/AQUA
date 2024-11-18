"""Simple catalog utility"""

import intake
from aqua.util import ConfigPath
from aqua.util.config import scan_catalog


def aqua_catalog(catalog=None, configdir=None, verbose=True):
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


def inspect_catalog(catalog=None, model=None, exp=None, source=None, verbose=False):
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
        verbose (bool, optional): Print the catalog information to the console. Defaults to False.

    Returns:
        list or dict:   A list of available items in the catalog, depending on the
                specified model and/or experiment, a list of variables or True/False.
                If multiple entries are matched in multiple catalogs
                it will return a dictionary with all the entries available
                for each catalog
            


    Raises:
        KeyError: If the input specifications are incorrect.
    """

    # safety check on argument sequence
    if model is None:
        if exp is not None or source is not None:
            raise ValueError("If 'exp' or 'source' is provided, 'model' must also be provided.")
    elif exp is None and source is not None:
        raise ValueError("If 'source' is provided, 'exp' must also be provided.")

    #getting the catalogs
    aquacats = aqua_catalog(catalog=catalog, verbose=False)

    # return a list of catalogs if nothing is provided
    if all(arg is None for arg in [catalog, model, exp, source]):
        if verbose:
            print(f"Catalog available in AQUA: {list(aquacats)}")
        return list(aquacats)
    
    # get all info from with the scan_catalog from config function
    infodict = {}
    for aquacat, cat in aquacats.items():
        check, level, avail = scan_catalog(cat, model, exp, source)
        infodict[aquacat] = {
            'check': check,
            'level': level,
            'avail': avail
        }

    # return information to the user
    for level in ['variables', 'source', 'exp', 'model']:
        status = find_string_in_dict(infodict, level)
        find = True
        if status:
            index = [t[1] for t in status]
            if model is not None and 'model' in index:
                print(f'Cannot find model {model}. Returning available models for installed catalogs')
                find = False
            if exp is not None and 'exp' in index:
                print(f'Cannot find exp {exp}. Returning available experiments for model {model}')
                find = False
            if source is not None and 'source' in index:
                print(f'Cannot find source {source}. Returning available source for experiment {exp}')
                find = False
        
            # multiple matches
            if len(status)>1:
                print(f"WARNING: inspect_catalog found multiple entries for the {level} key!")
                print('WARNING: Returning a dictionary instead of a list!')
                return {key: value['avail'] for key, value in infodict.items() if level in value['level']}
         
            
            printcat = status[0][0]
            if verbose:
                if not model and find:
                    print(f"Models available in catalog {printcat}:")
                if model and not exp and find:
                    print(f"Experiments available in catalog {printcat} for model {model}:")
                if model and exp and not source and find:
                    print(f"Sources available in catalog {printcat} for model {model} and exp {exp}:")
                if model and exp and source and find:
                    print(f"Source {source} for exp {exp} and model {model} in catalog {printcat} is found!")

            return infodict[printcat]['avail']

    # safety return
    return False
    


def find_string_in_dict(data, target_string):
    """Helper function"""
    matches = []

    for key, sub_dict in data.items():
 
        if sub_dict['level'] == target_string:
            matches.append((key, sub_dict['level']))

    return matches

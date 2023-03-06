import intake
from aqua.util import get_catalog_file

def catalogue(verbose=True, configdir=None):

    """Catalogue of available NextGEMS data (on Levante for now)"""

    _, catalog_file = get_catalog_file(configdir=configdir)

    cat = intake.open_catalog(catalog_file)
    if verbose:
        for model,vm in cat.items():
            for exp,ve in vm.items():
                print(model + '\t' + exp + '\t' + cat[model][exp].description)
                if exp != "grids":
                    for k in cat[model][exp]:
                        print('\t' + '- ' + k + '\t' + cat[model][exp].walk()[k]._description)
            print()
    return cat
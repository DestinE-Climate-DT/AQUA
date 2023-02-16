import intake
import os

def catalogue(verbose=True, configdir='config'):

    """Catalogue of available NextGEMS data (on Levante for now)"""

    catalog_file = os.path.join(configdir, "catalog.yaml")
    cat = intake.open_catalog(catalog_file)
    if verbose:
        for model,vm in cat.items():
            for exp,ve in vm.items():
                print(model + '\t' + exp + '\t' + cat[model][exp].description)
                if exp != "grids":
                    for k in cat[model][exp]:
                        print('\t' + '- ' + k + '\t' + cat[model][exp][k].description)
            print()
    return cat
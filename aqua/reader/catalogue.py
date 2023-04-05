"""Simple catalogue utility"""

import intake
from aqua.util import get_config_dir, get_machine, get_reader_filenames


def catalogue(verbose=True, configdir=None):
    """Catalogue of available NextGEMS data (on Levante for now)"""

    # get the config dir and the machine
    if not configdir:
        configdir = get_config_dir()
    machine = get_machine(configdir)

    # get configuration from the machine
    catalog_file, _, _ = get_reader_filenames(configdir, machine)

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

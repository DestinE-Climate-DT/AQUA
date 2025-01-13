"""Testing for the catalog generator"""

import subprocess
import os
import pytest
import logging
from aqua.util import load_yaml, dump_yaml

loglevel = "DEBUG"

def load_and_prepare(tmp_path, model, kind, reso, num_of_realizations=1):
    """
    Function to load and execute the catgen via the command line
    Starts from the config template, do some modification and verifies that
    everything is set up as expected.

    Args:
        tmp_path: temporary directory to store the data
        model: model to be checked (IFS-NEMO, IFS-FESOM, ICON)
        kind: data portfolio type (production, reduced)
        reso: resolution of the data (producution, lowres, intermediate, etc.)
        ensemble: number of realizations

    """

    config_file = 'tests/catgen/config-test-catgen.j2'

    # to parametriza on models, load the file, do jinja replacement and save it
    definitions = {
        'model': model, 
        'kind': kind, 
        'resolution': reso,
        'num_of_realizations': num_of_realizations
    } 
    config = load_yaml(config_file, definitions)
    model_config = f'{tmp_path}/test.yaml'
    
    dump_yaml(model_config, config)

    # Command to run
    command = ["aqua", "catgen", '-p', kind, '-c', model_config, '-l', loglevel]

    # Run the command with some error trap for debug
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info("Command succeeded with output: %s", result.stdout)
    except subprocess.CalledProcessError as e:
        # Handle the error and log the details
        logging.error("Command failed with error: %s", e)
        logging.error("Return code: %s", e.returncode)
        logging.error("stderr: %s", e.stderr)
        raise  # Re-raise the exception to allow for higher-level handling if necessary
    except Exception as e:
        # Catch other unforeseen exceptions
        logging.error("Unexpected error: %s", e)
        raise

    model = config['model']
    exp = config['exp']
    catalog_dir = config['catalog_dir']
    expected_path = os.path.join(config['repos']['Climate-DT-catalog_path'],
                                 'catalogs', catalog_dir, 'catalog', model)

    entry = os.path.join(expected_path, f'{exp}.yaml')
    assert os.path.exists(os.path.join(expected_path, 'main.yaml'))
    assert os.path.exists(entry)

    sources =  load_yaml(entry)
    #os.remove(os.path.join(expected_path, 'main.yaml'))
    #os.remove(entry)

    return sources

@pytest.mark.parametrize(('model,nsources,nocelevels'),
                        [('IFS-NEMO', 4, 75),
                         ('IFS-FESOM', 5, 47)])
@pytest.mark.catgen
def test_catgen_reduced(tmp_path, model, nsources, nocelevels):
    """test for production portfolio"""

    ensemble = 5

    sources = load_and_prepare(tmp_path=tmp_path, model=model,
                               kind='reduced', reso='intermediate',
                               num_of_realizations=ensemble)

    # check how many sources
    assert len(sources['sources']) == nsources

    # check if realization is correctly formatted
    assert "realization: '{{ realization }}'"

    # check number of vertical levels in the atmosphere
    if model == 'IFS-NEMO':
        grid, freq = 'lon-lat', 'monthly'
    elif model == 'IFS-FESOM':
        grid, freq = 'hpz7', 'daily'
    else:
        raise ValueError(f'{model} not supported!')
    assert len(sources['sources'][f'monthly-{grid}-atm3d']['metadata']['levels']) == 19

    # check number of vertical levels in the atmosphere
    assert len(sources['sources'][f'{freq}-{grid}-oce3d']['metadata']['levels']) == nocelevels

    # check ensembles are correctly produced
    assert sources['sources'][f'monthly-{grid}-atm3d']['parameters']['realization']['allowed'] == [*range(1, ensemble+1)]

@pytest.mark.parametrize(('model,nsources,nocelevels'),
                        [('IFS-NEMO', 30, 75),
                         ('IFS-FESOM', 34, 69),
                         ('ICON', 17, 72)])
@pytest.mark.catgen
def test_catgen_production(tmp_path, model, nsources, nocelevels):
    """test for production portfolio"""

    sources = load_and_prepare(tmp_path, model, 'production', 'production')

    # check how many sources
    assert len(sources['sources']) == nsources

    # check number of vertical levels in the atmosphere
    assert len(sources['sources']['hourly-hpz10-atm3d']['metadata']['levels']) == 19

    # check number of vertical levels in the atmosphere
    assert len(sources['sources']['daily-hpz10-oce3d']['metadata']['levels']) == nocelevels

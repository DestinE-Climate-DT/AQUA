"""Testing for the catalog generator"""

import subprocess
import os
import pytest
import logging
from pathlib import Path
from aqua.core.util import load_yaml, dump_yaml
from aqua.core.console.catgen import AquaFDBGenerator
from conftest import LOGLEVEL

loglevel = LOGLEVEL

def load_and_prepare(tmp_path, model, kind, reso, num_of_realizations=1):
    """
    Load configuration, execute catgen, and return generated catalog sources.
    
    This function ensures test isolation by using tmp_path for the catalog directory,
    preventing race conditions when tests run in parallel. All generated files are
    automatically cleaned up by pytest's tmp_path fixture.

    Args:
        tmp_path: Temporary directory provided by pytest (Path or str)
        model: Model to be checked (IFS-NEMO, IFS-FESOM, ICON)
        kind: Data portfolio type (minimal, reduced, full)
        reso: Resolution of the data (lowres, intermediate, production, etc.)
        num_of_realizations: Number of realizations for the ensemble
        
    Returns:
        dict: Catalog sources dictionary loaded from the generated YAML file
        
    Raises:
        subprocess.CalledProcessError: If catgen command fails
        AssertionError: If generated catalog files are not found
    """
    tmp_path = Path(tmp_path)
    config_template = Path('tests/catgen/config-test-catgen.j2')
    
    # Prepare configuration
    config = _prepare_config(config_template, model, kind, reso, num_of_realizations, tmp_path)
    
    # Setup isolated catalog directory structure
    _setup_catalog_directory(config, tmp_path)
    
    # Execute catgen command
    _run_catgen(config, kind, tmp_path)
    
    # Load and return generated sources
    return _load_generated_sources(config)


def _prepare_config(template_path, model, kind, reso, num_of_realizations, tmp_path):
    """Prepare configuration from template with test-specific values."""
    definitions = {
        'model': model,
        'kind': kind,
        'resolution': reso,
        'num_of_realizations': num_of_realizations,
        'expid': 'test'
    }
    config = load_yaml(str(template_path), definitions)
    
    # Use tmp_path for catalog directory to ensure test isolation
    catalog_base = tmp_path / 'Climate-DT-catalog'
    config['repos']['Climate-DT-catalog_path'] = str(catalog_base)
    
    return config


def _setup_catalog_directory(config, tmp_path):
    """Create isolated catalog directory structure and required files."""
    catalog_base = Path(config['repos']['Climate-DT-catalog_path'])
    catalog_dir = config['catalog_dir']
    catalog_yaml_path = catalog_base / 'catalogs' / catalog_dir / 'catalog.yaml'
    
    # Create directory structure
    catalog_yaml_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create initial catalog.yaml if it doesn't exist (required by catgen)
    if not catalog_yaml_path.exists():
        dump_yaml(str(catalog_yaml_path), {'sources': {}})


def _run_catgen(config, kind, tmp_path):
    """Execute the catgen command via subprocess."""
    config_path = tmp_path / 'test.yaml'
    dump_yaml(str(config_path), config)
    
    command = ["aqua", "catgen", '-p', kind, '-c', str(config_path), '-l', loglevel]
    
    try:
        result = subprocess.run(command, capture_output=True, text=True, check=True)
        logging.info("Command succeeded with output: %s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Command failed with error: %s", e)
        logging.error("Return code: %s", e.returncode)
        logging.error("stderr: %s", e.stderr)
        raise
    except Exception as e:
        logging.error("Unexpected error: %s", e)
        raise


def _load_generated_sources(config):
    """Load and validate the generated catalog sources."""
    catalog_path = Path(config['repos']['Climate-DT-catalog_path'])
    catalog_dir = config['catalog_dir']
    model = config['model']
    exp = config['exp']
    
    catalog_entry_dir = catalog_path / 'catalogs' / catalog_dir / 'catalog' / model.upper()
    entry_file = catalog_entry_dir / f'{exp}.yaml'
    main_yaml_file = catalog_entry_dir / 'main.yaml'
    
    # Validate that required files were generated
    assert main_yaml_file.exists(), f"main.yaml not found at {main_yaml_file}"
    assert entry_file.exists(), f"Catalog entry not found at {entry_file}"
    
    return load_yaml(str(entry_file))

@pytest.mark.parametrize(('model,nsources,nocelevels'),
                        [('IFS-NEMO', 4, 75)])
@pytest.mark.catgen
def test_catgen_minimal(tmp_path, model, nsources, nocelevels):
    """test for minimal portfolio"""

    ensemble = 5 

    sources = load_and_prepare(tmp_path=tmp_path, model=model,
                               kind='minimal', reso='lowres',
                               num_of_realizations=ensemble)

    # check how many sources
    assert len(sources['sources']) == nsources

    # check if realization is correctly formatted
    assert "realization: '{{ realization }}'"

    # check number of vertical levels in the atmosphere
    assert len(sources['sources'][f'monthly-hpz5-atm3d']['metadata']['levels']) == 19

    # check number of vertical levels in the ocean
    assert len(sources['sources'][f'monthly-hpz5-oce3d']['metadata']['levels']) == nocelevels

    # check ensembles are correctly produced
    assert sources['sources'][f'monthly-hpz5-atm3d']['parameters']['realization']['allowed'] == [*range(1, ensemble+1)]


@pytest.mark.parametrize(('model,nsources,nocelevels'),
                        [('IFS-NEMO', 9, 75)])
                        # ('IFS-FESOM', 5, 47)])
@pytest.mark.catgen
def test_catgen_reduced(tmp_path, model, nsources, nocelevels):
    """test for reduced portfolio"""

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
        grid, freq = 'hpz7', 'monthly'
    #elif model == 'IFS-FESOM':
    #   grid, freq = 'hpz7', 'daily'
    else:
        raise ValueError(f'{model} not supported!')
    assert len(sources['sources'][f'monthly-{grid}-atm3d']['metadata']['levels']) == 19

    # check number of vertical levels in the atmosphere
    assert len(sources['sources'][f'{freq}-{grid}-oce3d']['metadata']['levels']) == nocelevels

    # check ensembles are correctly produced
    assert sources['sources'][f'monthly-{grid}-atm3d']['parameters']['realization']['allowed'] == [*range(1, ensemble+1)]


@pytest.mark.parametrize(('model,nsources,nocelevels'),
                        [('IFS-NEMO', 28, 75),
                         ('IFS-FESOM', 31, 69),
                         ('ICON', 27, 72)])
@pytest.mark.catgen
def test_catgen_full(tmp_path, model, nsources, nocelevels):
    """test for full portfolio"""

    sources = load_and_prepare(tmp_path, model, 'full', 'production')

    # check how many sources
    assert len(sources['sources']) == nsources

    # check number of vertical levels in the atmosphere
    assert len(sources['sources']['hourly-hpz10-atm3d']['metadata']['levels']) == 19

    # check number of vertical levels in the atmosphere
    assert len(sources['sources']['daily-hpz10-oce3d']['metadata']['levels']) == nocelevels


MANDATORY_KEYS_TO_TEST = [
    "author",
    "model",
    "repos.data-portfolio_path",
]
@pytest.mark.parametrize("missing_key", MANDATORY_KEYS_TO_TEST)

@pytest.mark.catgen
def test_catgen_missing_key(tmp_path, missing_key):
    """
    Ensure that a ValueError is raised when a required configuration key is missing.
    """
    config_path = 'tests/catgen/config-test-catgen.j2'
    config = load_yaml(config_path)

    if missing_key.startswith("repos."):
        subkey = missing_key.split(".", 1)[1]
        if "repos" in config and subkey in config["repos"]:
            del config["repos"][subkey]
    else:
        config.pop(missing_key, None)

    dump_path = os.path.join(tmp_path, "test.yaml")
    dump_yaml(dump_path, config)

    with pytest.raises(ValueError, match="Missing required configuration keys"):
        AquaFDBGenerator(config_path=dump_path, data_portfolio="minimal")

import os
import pytest

from aqua.core.configurer import ConfigPath
from aqua import show_catalog_content

@pytest.mark.aqua
def test_config_plain():
    config = ConfigPath()
    assert config.filename == 'config-aqua.yaml'
    assert 'ci' in config.catalog_available

@pytest.mark.aqua
def test_config_paths():
    configfile = 'tests/config/config-aqua-custom.yaml'

    configdir = ConfigPath().get_config_dir()

    # Copy the file to the config directory
    os.system(f'cp {configfile} {configdir}')

    config = ConfigPath(catalog='ci', filename='config-aqua-custom.yaml', configdir=configdir)
    paths, _ = config.get_machine_info()

    assert paths['paths']['grids'] == 'pluto'

    # Remove the copied file
    os.system(f'rm {configdir}/config-aqua-custom.yaml')

@pytest.mark.aqua
def test_show_catalog_content_basic():
    """Test show_catalog_content with no filters."""
    results = show_catalog_content()

    assert isinstance(results, dict)
    # Check structure: catalog -> model -> exp -> list of sources
    for _, catalog_data in results.items():
        assert isinstance(catalog_data, dict)
        for _, model_data in catalog_data.items():
            assert isinstance(model_data, dict)
            for _, sources in model_data.items():
                assert isinstance(sources, list)

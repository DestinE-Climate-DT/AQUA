import os
import shutil

import pytest

from aqua import show_catalog_content
from aqua.core.configurer import ConfigCatalog, ConfigContext


@pytest.mark.aqua
def test_config_plain():
    config = ConfigContext()
    assert "config-aqua.yaml" == os.path.basename(config.config_file)
    assert "ci" in config.config_dict["catalog"]


@pytest.mark.aqua
def test_config_paths():
    configfile = "tests/config/config-aqua-custom.yaml"

    configdir = ConfigContext().get_config_dir()

    # Copy the file to the config directory
    shutil.copy(configfile, configdir)

    config = ConfigCatalog(catalog="ci", paths=ConfigContext(filename="config-aqua-custom.yaml", configdir=configdir))
    paths, _ = config.get_machine_info()

    assert paths["paths"]["grids"] == "pluto"

    # Remove the copied file
    os.remove(f"{configdir}/config-aqua-custom.yaml")


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

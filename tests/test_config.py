import os
import pytest

from aqua.util import ConfigPath

@pytest.mark.aqua
def test_config_plain():
    config = ConfigPath()
    assert config.filename == 'config-aqua.yaml'
    assert config.catalog == 'ci'

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

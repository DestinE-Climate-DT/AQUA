import pytest

from aqua.util import ConfigPath

@pytest.mark.aqua
def test_config_plain():
    config = ConfigPath()
    assert config.filename == 'config-aqua.yaml'
    assert config.catalog == 'ci'

# @pytest.mark.aqua
# def test_config_ci():
#     config = ConfigPath(configdir='./config', catalog='ci')
#     assert config.filename == 'config-aqua.yaml'
#     assert config.catalog == 'ci'

import pytest
import sys
sys.path.insert(1, './diagnostics/dummy/')

# test parameters if needed
approx_rel = 1e-4

@pytest.mark.dummy
@pytest.mark.parametrize("module_name", ['dummy_class'])
def test_import(module_name):
    """
    """
    try:
        __import__(module_name)
    except ImportError:
        assert False, "Module {} could not be imported".format(module_name)


import pytest
from aqua.diagnostics import Teleconnection

loglevel = 'DEBUG'

@pytest.mark.teleconnections
def test_class_MJO():
    """TTest that the MJO class is not yet implemented"""
    telecname = 'MJO'
    model = 'IFS'
    exp = 'test-tco79'
    source = 'teleconnections'
    interface = 'teleconnections-ci'

    with pytest.raises(NotImplementedError):
        Teleconnection(model=model, exp=exp, source=source,
                       loglevel=loglevel, telecname=telecname,
                       interface=interface)

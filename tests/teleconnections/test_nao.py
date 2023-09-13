import pytest

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.teleconnections
def test_class_NAO():
    """
    Test that the NAO class works
    """
    from teleconnections.tc_class import Teleconnection

    telecname = 'NAO'
    model = 'IFS'
    exp = 'test-tco79'
    source = 'teleconnections'

    telec = Teleconnection(model=model, exp=exp, source=source,
                           loglevel=loglevel, telecname=telecname,
                           configdir="diagnostics/teleconnections/config")

    telec.evaluate_index()
    assert telec.index[4].values == pytest.approx(0.21909582, rel=approx_rel)

    telec.evaluate_regression()
    assert telec.regression.isel(lon=4, lat=23).values == pytest.approx(0.77142069, rel=approx_rel)

    telec.evaluate_correlation()
    assert telec.correlation.isel(lon=4, lat=23).values == pytest.approx(0.00220419, rel=approx_rel)

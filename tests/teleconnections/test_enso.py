import pytest

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.teleconnections
def test_class_ENSO():
    """
    Test that the ENSO class works
    """
    from teleconnections.tc_class import Teleconnection

    telecname = 'ENSO_test'
    model = 'IFS'
    exp = 'test-tco79'
    source = 'teleconnections'

    telec = Teleconnection(model=model, exp=exp, source=source,
                           loglevel=loglevel, telecname=telecname,
                           configdir="diagnostics/teleconnections/config")

    telec.evaluate_index()
    assert telec.index[4].values == pytest.approx(-1.08063012, rel=approx_rel)

    telec.evaluate_regression()
    assert telec.regression.isel(lon=4, lat=23).values == pytest.approx(-0.13339995, rel=approx_rel)

    telec.evaluate_correlation()
    assert telec.correlation.isel(lon=4, lat=23).values == pytest.approx(-0.06538111, rel=approx_rel)

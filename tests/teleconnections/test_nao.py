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
    interface = 'teleconnections-ci'

    telec = Teleconnection(model=model, exp=exp, source=source,
                           loglevel=loglevel, telecname=telecname,
                           interface=interface,
                           configdir="diagnostics/teleconnections/config")

    telec.evaluate_index()
    assert telec.index[4].values == pytest.approx(0.21909582, rel=approx_rel)

    reg = telec.evaluate_regression()
    assert reg.isel(lon=4, lat=23).values == pytest.approx(0.77142069,
                                                           rel=approx_rel)

    cor = telec.evaluate_correlation()
    assert cor.isel(lon=4, lat=23).values == pytest.approx(0.00220419,
                                                           rel=approx_rel)

    reg_DJF = telec.evaluate_regression(season='DJF')
    assert reg_DJF.isel(lon=4, lat=23).values == pytest.approx(80.99873476,
                                                               rel=approx_rel)

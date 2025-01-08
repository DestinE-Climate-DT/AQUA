import pytest
from aqua.exceptions import NoDataError
from aqua.diagnostics import Teleconnection

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

telecname = 'ENSO'
model = 'ERA5'
exp = 'era5-hpz3'
source = 'monthly'
interface = 'teleconnections-destine'
regrid = 'r100'


@pytest.mark.teleconnections
def test_class_ENSO():
    """
    Test that the ENSO class works
    """
    telec = Teleconnection(model=model, exp=exp, source=source,
                           loglevel=loglevel, telecname=telecname,
                           interface=interface, regrid=regrid)

    telec.evaluate_index()
    assert telec.index[4].values == pytest.approx(-0.29945775, rel=approx_rel)

    with pytest.raises(NoDataError):
        telec.retrieve(var='pippo')

    reg = telec.evaluate_regression()
    assert reg.isel(lon=4, lat=23).values == pytest.approx(0.01764006,
                                                           rel=approx_rel)

    cor = telec.evaluate_correlation()
    assert cor.isel(lon=4, lat=23).values == pytest.approx(0.009964,
                                                           rel=approx_rel)

    cor_tprate = telec.evaluate_correlation(var='tprate')
    assert cor_tprate.isel(lon=4, lat=23).values == pytest.approx(-0.04669953,
                                                                  rel=approx_rel)


@pytest.mark.teleconnections
def test_teleconnections_unknown():
    """Test unknown teleconnection"""
    with pytest.raises(ValueError):
        Teleconnection(model=model, exp=exp, source=source,
                       loglevel=loglevel, telecname='pippo',
                       interface=interface)

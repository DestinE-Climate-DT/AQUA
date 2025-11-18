import os
import matplotlib
import pytest
from aqua.diagnostics.teleconnections import MJO, PlotMJO
from conftest import APPROX_REL, LOGLEVEL, DPI

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL

@pytest.mark.diagnostics
def test_MJO(tmp_path):
    """
    Test that the MJO class works
    """
    init_dict = {
        'model': 'ERA5',
        'exp': 'era5-hpz3',
        'source': 'monthly',
        'loglevel': loglevel,
        'regrid': 'r200'
    }

    mjo = MJO(**init_dict)
    mjo.retrieve()
    assert mjo.data is not None, "Data should not be None"

    mjo.compute_hovmoller(day_window=30)
    assert mjo.data_hovmoller is not None, "Hovmoller data should not be None"

    plot = PlotMJO(data=mjo.data_hovmoller, loglevel=loglevel, outputdir=tmp_path)
    fig = plot.plot_hovmoller()
    assert isinstance(fig, matplotlib.figure.Figure), "Figure should be a matplotlib Figure"
    plot.save_plot(fig, diagnostic_product='hovmoller', metadata={'description': 'MJO Hovmoller plot'}, dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'png', 'mjo.hovmoller.ci.ERA5.era5-hpz3.r1.png'))) is True
    plot.save_plot(fig, diagnostic_product='hovmoller', format='pdf', metadata={'description': 'MJO Hovmoller plot'}, dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'pdf', 'mjo.hovmoller.ci.ERA5.era5-hpz3.r1.pdf'))) is True

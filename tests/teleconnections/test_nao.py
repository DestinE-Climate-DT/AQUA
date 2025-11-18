import os
import matplotlib
import pytest
from aqua.diagnostics.teleconnections import NAO, PlotNAO
from conftest import APPROX_REL, LOGLEVEL, DPI

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL


@pytest.mark.diagnostics
def test_NAO(tmp_path):
    """
    Test that the NAO class works
    """
    init_dict = {
        'model': 'IFS',
        'exp': 'test-tco79',
        'source': 'teleconnections',
        'loglevel': loglevel
    }

    # Init test
    nao = NAO(**init_dict)
    nao.retrieve()
    assert nao.data is not None, "Data should not be None"

    # Index computation and saving
    nao.compute_index()
    assert nao.index is not None, "Index should not be None"
    assert nao.index[4].values == pytest.approx(0.21909582, rel=approx_rel)

    nao.save_netcdf(nao.index, diagnostic='nao', diagnostic_product='index',
                    outputdir=tmp_path)
    netcdf_path = os.path.join(tmp_path, 'netcdf')
    filename = 'nao.index.ci.IFS.test-tco79.r1.nc'
    assert (os.path.exists(os.path.join(netcdf_path, filename))) is True

    # Regression and correlation computation
    reg_DJF = nao.compute_regression(season='DJF')
    assert reg_DJF.isel(lon=4, lat=23).values == pytest.approx(80.99873476, rel=approx_rel)
    cor = nao.compute_correlation()
    assert cor.isel(lon=4, lat=23).values == pytest.approx(0.00220419, rel=approx_rel)

    # Plotting
    plot_ref = PlotNAO(loglevel=loglevel, indexes=nao.index,
                       ref_indexes=nao.index, outputdir=tmp_path)

    # Index plotting
    fig, _ = plot_ref.plot_index()
    description = plot_ref.set_index_description()
    assert description == 'NAO index for IFS test-tco79 using reference data from IFS test-tco79.'
    assert isinstance(fig, matplotlib.figure.Figure), "Figure should be a matplotlib Figure"
    plot_ref.save_plot(fig, diagnostic_product='index', metadata={'description': description}, dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'png', 'nao.index.ci.IFS.test-tco79.r1.ci.IFS.test-tco79.png'))) is True

    # Regression plotting
    reg_DJF.load()
    fig_reg = plot_ref.plot_maps(maps=reg_DJF, ref_maps=reg_DJF, statistic='regression')
    assert isinstance(fig_reg, matplotlib.figure.Figure), "Figure should be a matplotlib Figure"
    description = plot_ref.set_map_description(maps=reg_DJF, ref_maps=reg_DJF, statistic='regression')
    assert description == 'NAO regression map (Mean sea level pressure) IFS test-tco79 (DJF) compared to IFS test-tco79. The contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.' # noqa: E501
    plot_ref.save_plot(fig_reg, diagnostic_product='regression_DJF', metadata={'description': description}, format='pdf', dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'pdf', 'nao.regression_djf.ci.IFS.test-tco79.r1.ci.IFS.test-tco79.pdf'))) is True

    # Correlation plotting
    plot_single = PlotNAO(loglevel=loglevel, indexes=nao.index, outputdir=tmp_path)
    cor.load()
    fig_cor = plot_single.plot_maps(maps=cor, statistic='correlation')
    assert isinstance(fig_cor, matplotlib.figure.Figure), "Figure should be a matplotlib Figure"
    description = plot_single.set_map_description(maps=cor, statistic='correlation')
    assert description == 'NAO correlation map (Correlation of Mean sea level pressure with index evaluated with Mean sea level pressure) IFS test-tco79.'
    plot_single.save_plot(fig_cor, diagnostic_product='correlation', metadata={'description': description}, format='pdf', dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'pdf', 'nao.correlation.ci.IFS.test-tco79.r1.pdf'))) is True

    # Not implemented maps
    fig_not_implemented = plot_ref.plot_maps(maps=[reg_DJF, reg_DJF], ref_maps=reg_DJF, statistic='not_implemented')
    assert fig_not_implemented is None, "Plotting with not implemented statistic should return None"
    fig_not_implemented = plot_ref.plot_maps(maps=reg_DJF, ref_maps=[reg_DJF, reg_DJF], statistic='regression')
    assert fig_not_implemented is None, "Plotting with not implemented statistic should return None"
    fig_not_implemented = plot_ref.plot_maps(maps=[reg_DJF, reg_DJF], ref_maps=[reg_DJF, reg_DJF], statistic='regression')
    assert fig_not_implemented is None, "Plotting with not implemented statistic should return None"
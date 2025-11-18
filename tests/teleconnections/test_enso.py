import os
import matplotlib
import pytest
from aqua.exceptions import NotEnoughDataError
from aqua.diagnostics.teleconnections import ENSO, PlotENSO
from conftest import APPROX_REL, LOGLEVEL, DPI

# pytest approximation, to bear with different machines
approx_rel = APPROX_REL
loglevel = LOGLEVEL


@pytest.mark.diagnostics
def test_ENSO(tmp_path):
    """
    Test that the NAO class works
    """
    init_dict = {
        'model': 'ERA5',
        'exp': 'era5-hpz3',
        'source': 'monthly',
        'loglevel': loglevel,
        'regrid': 'r100'
    }

    # Init test
    enso = ENSO(**init_dict)

    with pytest.raises(NotEnoughDataError):
        assert enso.compute_index(), "Data not retrieved should raise NotEnoughDataError"

    enso.retrieve()
    assert enso.data is not None, "Data should not be None"

    with pytest.raises(ValueError):
        assert enso.compute_regression(season='annual'), "Regression without index should raise ValueError"

    # Index computation and saving
    enso.compute_index()
    assert enso.index is not None, "Index should not be None"
    assert enso.index[4].values == pytest.approx(-0.29945775, rel=approx_rel)

    enso.save_netcdf(enso.index, diagnostic='enso', diagnostic_product='index',
                     outputdir=tmp_path)
    netcdf_path = os.path.join(tmp_path, 'netcdf')
    filename = 'enso.index.ci.ERA5.era5-hpz3.r1.nc'
    assert (os.path.exists(os.path.join(netcdf_path, filename))) is True

    # Regression and correlation computation
    reg = enso.compute_regression(season='annual')
    assert reg.isel(lon=4, lat=23).values == pytest.approx(0.01764006, rel=approx_rel)
    cor = enso.compute_correlation()
    assert cor.isel(lon=4, lat=23).values == pytest.approx(0.009964, rel=approx_rel)
    cor_tprate = enso.compute_correlation(var='tprate')
    assert cor_tprate.isel(lon=4, lat=23).values == pytest.approx(-0.04669953, rel=approx_rel)

    # Plotting
    plot_ref = PlotENSO(loglevel=loglevel, indexes=enso.index,
                        ref_indexes=enso.index, outputdir=tmp_path)
    
    # Index plotting
    fig, _ = plot_ref.plot_index()
    description = plot_ref.set_index_description()
    assert description == 'ENSO3.4 index for ERA5 era5-hpz3 using reference data from ERA5 era5-hpz3.'
    assert isinstance(fig, matplotlib.figure.Figure), "Figure should be a matplotlib Figure"
    plot_ref.save_plot(fig, diagnostic_product='index', metadata={'description': description}, dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'png', 'enso.index.ci.ERA5.era5-hpz3.r1.ci.ERA5.era5-hpz3.png'))) is True

    # Regression plotting
    reg.load()
    fig_reg = plot_ref.plot_maps(maps=reg, ref_maps=reg, statistic='regression')
    assert isinstance(fig_reg, matplotlib.figure.Figure)
    description = plot_ref.set_map_description(maps=reg, ref_maps=reg, statistic='regression')
    assert description == 'ENSO3.4 regression map (tos) ERA5 era5-hpz3 compared to ERA5 era5-hpz3. The contour lines are the model regression map and the filled contour map is the difference between the model and the reference regression map.'  # noqa: E501
    plot_ref.save_plot(fig_reg, diagnostic_product='regression_annual', metadata={'description': description}, format='pdf', dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'pdf', 'enso.regression_annual.ci.ERA5.era5-hpz3.r1.ci.ERA5.era5-hpz3.pdf'))) is True

    # Correlation plotting
    plot_single = PlotENSO(loglevel=loglevel, indexes=enso.index, outputdir=tmp_path)
    cor.load()
    fig_cor = plot_single.plot_maps(maps=cor, statistic='correlation')
    assert isinstance(fig_cor, matplotlib.figure.Figure)
    description = plot_single.set_map_description(maps=cor, statistic='correlation')
    assert description == 'ENSO3.4 correlation map (Correlation of Sea surface temperature with index evaluated with Sea surface temperature) ERA5 era5-hpz3.'
    plot_single.save_plot(fig_cor, diagnostic_product='correlation', metadata={'description': description}, format='pdf', dpi=DPI)
    assert (os.path.exists(os.path.join(tmp_path, 'pdf', 'enso.correlation.ci.ERA5.era5-hpz3.r1.pdf'))) is True

    # We add the attribute to increase coverage covering also the season case
    reg.attrs['AQUA_season'] = 'annual'
    cor.attrs['AQUA_season'] = 'annual'

    # Model is a list, no reference
    fig_reg_no_ref = plot_ref.plot_maps(maps=[reg, reg], ref_maps=None, statistic='regression')
    assert isinstance(fig_reg_no_ref, matplotlib.figure.Figure)

    # Model is a list, reference is a single map
    fig_reg_single_ref = plot_ref.plot_maps(maps=[reg, reg], ref_maps=reg, statistic='regression')
    assert isinstance(fig_reg_single_ref, matplotlib.figure.Figure)

    # Model is a single map, reference is a list
    reg2 = reg + reg  # To ensure reg is a different map
    fig_reg_list_ref = plot_ref.plot_maps(maps=reg2, ref_maps=[reg, reg], statistic='regression')
    assert isinstance(fig_reg_list_ref, matplotlib.figure.Figure)

    # Not implemented maps
    fig_not_implemented = plot_ref.plot_maps(maps=[reg, reg], ref_maps=[reg, reg], statistic='not_implemented')
    assert fig_not_implemented is None, "Plotting with not implemented statistic should return None"

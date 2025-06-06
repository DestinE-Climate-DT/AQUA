import pytest
import numpy as np
from aqua import Reader
from aqua.diagnostics import GlobalBiases

# Tolerance for numerical comparisons
approx_rel = 1e-4
loglevel = 'DEBUG'

reader = Reader(model='ERA5', exp='era5-hpz3', source='monthly', catalog='ci', regrid='r100')
data = reader.retrieve()
data = reader.regrid(data)


#@pytest.mark.diagnostics
def test_global_bias():
    """
    Test for global bias computation.
    """
    global_biases = GlobalBiases(data=data, data_ref=data, var_name='tprate')
    fig, ax, bias = global_biases.plot_bias(vmin=None, vmax=None)
    assert fig is not None
    assert ax is not None
    assert np.allclose(np.nanmean(bias.values), 0, atol=approx_rel)


#@pytest.mark.diagnostics
def test_seasonal_bias():
    """
    Test for seasonal bias computation.
    """
    global_biases = GlobalBiases(data=data, data_ref=data, var_name='2t')
    fig, bias = global_biases.plot_seasonal_bias(vmin=-15, vmax=15)
    assert fig is not None
    for season in ['DJF', 'MAM', 'JJA', 'SON']:
        assert np.allclose(np.nanmean(bias[season].values), 0, atol=approx_rel)


#@pytest.mark.diagnostics
def test_plev():
    """
    Test for 3D variable bias computation (e.g., specific humidity at 850 hPa).
    """
    global_biases = GlobalBiases(data=data, data_ref=data, var_name='q', plev=85000)
    fig, ax, bias = global_biases.plot_bias(vmin=-0.002, vmax=0.002)
    assert fig is not None
    assert ax is not None
    assert np.allclose(np.nanmean(bias.values), 0, atol=approx_rel)
    assert getattr(bias, "plev", None) == 85000


# @pytest.mark.diagnostics
def test_vertical_bias():
    """
    Test for vertical bias computation.
    """
    global_biases = GlobalBiases(data=data, data_ref=data, var_name='q')
    fig, ax, bias = global_biases.plot_vertical_bias(vmin=-0.002, vmax=0.002)
    assert fig is not None
    assert ax is not None
    assert np.allclose(np.nanmean(bias.values), 0, atol=approx_rel)

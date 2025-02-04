import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import GregoryPlot

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.timeseries
def test_class_gregory():
    """
    Test that the GregoryPlot class works
    """
    catalogs = ['ci']
    models = ['ERA5']
    exps = ['era5-hpz3']
    sources = ['monthly']

    gp = GregoryPlot(catalogs=catalogs, models=models, exps=exps, sources=sources,
                     loglevel=loglevel, ref=False)

    assert gp.ts_name == '2t'

    gp.retrieve_data()

    assert gp.data_ts_mon[0] is not None
    assert gp.data_ts_annual[0] is not None

    # No reference yet
    gp.retrieve_ref()

    gp.plot()
    gp.save_netcdf()

    # We want to assert the plot is created
    pdf_file = './pdf/timeseries.gregory_plot.ci.ERA5.era5-hpz3.pdf'
    netcdf_file = './netcdf/timeseries.gregory_plot.ci.ERA5.era5-hpz3.frequency_monthly.nc'
    netcdf_file_annual = './netcdf/timeseries.gregory_plot.ci.ERA5.era5-hpz3.frequency_annual.nc'

    assert os.path.exists(pdf_file) is True
    assert os.path.exists(netcdf_file) is True
    assert os.path.exists(netcdf_file_annual) is True

    gp.cleanup()

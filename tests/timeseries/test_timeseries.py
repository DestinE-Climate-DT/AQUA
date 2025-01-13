import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import Timeseries

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

models = ['IFS']
exps = ['test-tco79']
sources = ['teleconnections']
var = 'msl'
lon_limits = [-100, 100]
lat_limits = [-30, 30]
plot_ref_kw = {'catalog': 'ci', 'model': 'IFS', 'exp': 'test-tco79', 'source': 'teleconnections'}


@pytest.mark.timeseries
def test_class_timeseries():
    """
    Test that the timeseries class works
    """

    ts = Timeseries(var=var, models=models, exps=exps, sources=sources,
                    loglevel=loglevel, plot_ref_kw=plot_ref_kw,
                    lon_limits=lon_limits, lat_limits=lat_limits)

    assert ts.var == var

    ts.retrieve_data()

    assert ts.data_annual is not None
    assert ts.data_mon is not None

    ts.retrieve_ref(extend=True)

    assert ts.ref_ann is not None
    assert ts.ref_mon is not None

    ts.plot()

    # We want to assert the plot is created
    pdf_file = './pdf/timeseries.timeseries.ci.IFS.test-tco79.msl.lat_limits__lat-30_30.lon_limits__lon-100_100.pdf'
    png_file = './png/timeseries.timeseries.ci.IFS.test-tco79.msl.lat_limits__lat-30_30.lon_limits__lon-100_100.png'

    assert os.path.exists(pdf_file) is True
    assert os.path.exists(png_file) is True

    ts.save_netcdf()

    # We want to assert the netcdf file is created
    assert os.path.exists('./netcdf/timeseries.timeseries.ci.IFS.test-tco79.msl.frequency_annual.lat_limits__lat-30_30.lon_limits__lon-100_100.nc') is True
    assert os.path.exists('./netcdf/timeseries.timeseries.ci.IFS.test-tco79.msl.frequency_monthly.lat_limits__lat-30_30.lon_limits__lon-100_100.nc') is True

    # Data and reference are the same so the difference should be zero.
    # I check opening the netcdf file
    data_annual = xr.open_dataset('./netcdf/timeseries.timeseries.ci.IFS.test-tco79.msl.frequency_annual.lat_limits__lat-30_30.lon_limits__lon-100_100.nc')
    data_ref_annual = xr.open_dataset('./netcdf/timeseries.timeseries.ci.IFS.test-tco79.msl.frequency_annual.lat_limits__lat-30_30.lon_limits__lon-100_100.nc')
    diff = data_annual - data_ref_annual
    assert pytest.approx(diff[ts.var].isel(time=0).values, rel=approx_rel) == 0

    ts.cleanup()

    # data.annual and data.mon have been deleted
    with pytest.raises(AttributeError):
        assert ts.data_annual is None
        assert ts.data_mon is None

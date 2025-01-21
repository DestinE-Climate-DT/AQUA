import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import Timeseries

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'


@pytest.mark.timeseries
def test_class_timeseries():
    """
    Test that the timeseries class works
    """
    models = ['IFS']
    exps = ['test-tco79']
    sources = ['teleconnections']
    var = 'msl'
    lon_limits = [-100, 100]
    lat_limits = [-30, 30]
    plot_ref_kw = {'catalog': 'ci', 'model': 'IFS', 'exp': 'test-tco79', 'source': 'teleconnections'}

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


@pytest.mark.timeseries
def test_timeseries_regions():

    models = ['ERA5']
    exps = ['era5-hpz3']
    sources = ['monthly']
    var = '86400*tprate'
    region = 'nh'

    ts = Timeseries(var=var, models=models, exps=exps, sources=sources,
                    loglevel=loglevel, region=region, formula=True,
                    save=False, regrid='r100',
                    longname='Total precipitation rate',
                    units='mm/day')

    assert ts.lon_limits == [-180, 180]
    assert ts.lat_limits == [30, 90]

    ts.retrieve_data()
    # we have no reference data
    ts.retrieve_ref(extend=False)
    ts.plot()

    assert ts.data_annual[0].isel(time=4).values == pytest.approx(2.21320749, rel=approx_rel)

    with pytest.raises(KeyError):
        ts = Timeseries(var=var, models=models, exps=exps, sources=sources,
                        loglevel=loglevel, region='topolinia', formula=True)

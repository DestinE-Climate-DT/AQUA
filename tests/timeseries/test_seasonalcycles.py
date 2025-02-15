import os
import pytest
import xarray as xr
from aqua.diagnostics.timeseries import SeasonalCycle

# pytest approximation, to bear with different machines
approx_rel = 1e-4
loglevel = 'DEBUG'

catalogs = ['ci']
models = ['IFS']
exps = ['test-tco79']
sources = ['teleconnections']
var = 'msl'
lon_limits = [-100, 100]
lat_limits = [-30, 30]
plot_ref_kw = {'catalog': 'ci', 'model': 'IFS', 'exp': 'test-tco79', 'source': 'teleconnections'}


@pytest.mark.timeseries
def test_class_seasonalcycle():
    """
    Test that the timeseries class works
    """

    sc = SeasonalCycle(var=var, models=models, exps=exps, sources=sources, catalogs=catalogs,
                       loglevel=loglevel, plot_ref_kw=plot_ref_kw,
                       lon_limits=lon_limits, lat_limits=lat_limits)

    assert sc.var == var

    sc.retrieve_data()

    assert sc.data_annual == []
    assert sc.data_mon is not None

    sc.retrieve_ref()

    assert sc.ref_ann is None
    assert sc.ref_mon is not None

    sc.seasonal_cycle()

    assert sc.cycle is not None

    sc.plot()

    # We want to assert the plot is created
    pdf_file = './pdf/timeseries.seasonalcycle.ci.IFS.test-tco79.msl.IFS.test-tco79.lat_limits__lat-30_30.lon_limits__lon-100_100.pdf' # noqa
    png_file = './png/timeseries.seasonalcycle.ci.IFS.test-tco79.msl.IFS.test-tco79.lat_limits__lat-30_30.lon_limits__lon-100_100.png'

    assert os.path.exists(pdf_file) is True
    assert os.path.exists(png_file) is True

    sc.save_netcdf()

    # We want to assert the netcdf file is created
    assert os.path.exists('./netcdf/timeseries.seasonalcycle.ci.IFS.test-tco79.msl.frequency_monthly.lat_limits__lat-30_30.lon_limits__lon-100_100.nc') is True

    # TODO: Data and reference are the same so the difference should be zero.

    sc.cleanup()

    # data.cycle have been deleted
    with pytest.raises(AttributeError):
        assert sc.cycle is None

import pytest
from aqua.diagnostics.core import Diagnostic
from conftest import LOGLEVEL

loglevel = LOGLEVEL


@pytest.mark.aqua
def test_class_diagnostic(tmp_path):
    """
    Test the diagnostic class
    """
    catalog = None
    model = 'ERA5'
    exp = 'era5-hpz3'
    source = 'monthly'
    var = 'tcc'
    regrid = 'r100'
    startdate = '19000101'
    enddate = '19910101'
    outputdir = tmp_path

    diag = Diagnostic(catalog=catalog, model=model, exp=exp, source=source,
                      regrid=regrid, startdate=startdate, enddate=enddate,
                      loglevel=loglevel)

    assert diag.model == model
    assert diag.exp == exp
    assert diag.source == source
    assert diag.regrid == regrid
    assert diag.startdate == startdate
    assert diag.enddate == enddate

    diag.retrieve(var=var)

    assert diag.catalog == 'ci'
    assert diag.data is not None

    data_sel = diag.data.isel(time=0)

    diag.save_netcdf(data=data_sel, diagnostic='test',
                     diagnostic_product='save',
                     outputdir=outputdir,
                     rebuild=True)

    assert outputdir.joinpath('netcdf/test.save.ci.ERA5.era5-hpz3.r1.nc').exists()
    
    #test select_region
    region, diagnostic = None, None #testing when region is None
    region, lon_limits, lat_limits = diag.select_region(region=region, diagnostic=diagnostic)
    assert region == None
    assert lon_limits == None
    assert lat_limits == None
    
    region = 'io'
    diagnostic = "ocean3d"
    region, lon_limits, lat_limits = diag.select_region(region=region, diagnostic=diagnostic)
    assert region == "Indian Ocean"
    assert lon_limits == [30, 110]
    assert lat_limits == [-30, 30]

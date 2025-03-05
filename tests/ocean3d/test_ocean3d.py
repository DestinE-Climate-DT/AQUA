import os
import pytest
import xarray as xr
from aqua import Reader
from aqua.util import load_yaml, ConfigPath
from ocean3d import check_variable_name

from ocean3d import stratification
from ocean3d import mld

from ocean3d import hovmoller_plot
from ocean3d import time_series
from ocean3d import multilevel_trend
from ocean3d import zonal_mean_trend

@pytest.fixture
def common_setup(tmp_path):
    """Fixture to set up common configuration and test data."""
    Configurer = ConfigPath()
    loglevel = 'warning'
    catalog = 'ci'
    exp = 'hpz3'
    model = 'FESOM'
    source = 'monthly'
    region = 'Indian Ocean'
    reader = Reader(model=model, exp=exp, source= source, catalog= catalog, regrid='r100')
    data = reader.retrieve(var="thetao")
    data = reader.regrid(data)
    data1 = data.rename({"thetao": "so"})
    data = data.merge(data1)

    return {
        "loglevel": loglevel,
        "outputdir": tmp_path,
        "model": model,
        "exp": exp,
        "source": source,
        "data": data,
        "region": region,
    }

@pytest.mark.ocean3d
def test_check_variable_name(common_setup):
    """Test variable name checking and transformations."""
    setup = common_setup
    data = check_variable_name(setup["data"], loglevel=setup["loglevel"])

    # Ensure required variables exist
    assert 'so' in data, "Variable 'so' not found in dataset"
    assert 'thetao' in data, "Variable 'thetao' not found in dataset"

    # Ensure units are converted correctly
    assert data['thetao'].attrs.get('units') == 'degC', "Units not converted to Celsius"

    # Ensure expected dimensions exist
    assert 'lev' in data.dims, "Dimension 'lev' missing in dataset"

    print("Test passed successfully!")


@pytest.mark.ocean3d
def test_hovmoller_plot(common_setup):
    setup = common_setup
    setup["data"] = check_variable_name(setup["data"], loglevel=setup["loglevel"])
    hovmoller_plot_init = hovmoller_plot(setup)
    hovmoller_plot_init.plot()
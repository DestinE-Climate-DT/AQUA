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
from pathlib import Path

@pytest.fixture
def common_setup(tmp_path):
    """Fixture to set up common configuration and test data."""
    Configurer = ConfigPath()
    loglevel = 'warning'
    catalog = 'ci'
    exp = 'hpz3'
    model = 'FESOM'
    source = 'monthly-3d'
    region = 'Indian Ocean'
    output_dir = "./tmp"
    output = True
    reader = Reader(model=model, exp=exp, source= source, catalog= catalog, regrid='r100')
    data = reader.retrieve(var=["thetao", "so"])
    data = reader.regrid(data)

    return {
        "loglevel": loglevel,
        "outputdir": tmp_path,
        "model": model,
        "exp": exp,
        "source": source,
        "data": data,
        "region": region,
        "output_dir" : "./tmp",
        "output" : True
    }

@pytest.mark.diagnostics
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


@pytest.fixture
def hovmoller_instance(common_setup):
    """Fixture to initialize hovmoller plot instance"""
    setup = common_setup
    setup["data"] = check_variable_name(setup["data"], loglevel=setup["loglevel"])
    hovmoller_plot_init = hovmoller_plot(setup)
    return hovmoller_plot_init
    
@pytest.mark.diagnostics
def test_hovmoller_data(hovmoller_instance):
    """Test data loading for hovmoller plot."""
    hovmoller_instance.data_for_hovmoller_lev_time_plot()
    for i in range(1, len(hovmoller_instance.plot_info) + 1):
        assert hovmoller_instance.plot_info[i]["data"] is not None, f"Data not loaded for hovmoller plot hovmoller_instance.plot_info[1]['type']"
    print("Test passed successfully!")
    
@pytest.mark.diagnostics
def test_hovmoller_plot(hovmoller_instance):
    """Test hovmoller plot generation."""
    hovmoller_instance.plot()
    output_file = Path(f"{hovmoller_instance.output_dir}/pdf/FESOM-hpz3-monthly-3d_hovmoller_plot_indian_ocean.pdf")
    assert output_file.exists(), "Plot not generated"
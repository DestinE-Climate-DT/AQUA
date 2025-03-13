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

approx_rel = 1e-4

@pytest.fixture
def common_setup(tmp_path):
    """Fixture to set up common configuration and test data."""
    loglevel = 'DEBUG'
    catalog = 'ci'
    exp = 'hpz3'
    model = 'FESOM'
    source = 'monthly-3d'
    region = 'Indian Ocean'
    output_dir = tmp_path
    output = True
    reader = Reader(model=model, exp=exp, source= source, catalog= catalog, regrid='r100')
    data = reader.retrieve(var=["thetao", "so"])
    data = reader.regrid(data)

    return {
        "loglevel": loglevel,
        "model": model,
        "exp": exp,
        "source": source,
        "data": data,
        "region": region,
        "output_dir" : output_dir,
        "output" : output
    }

@pytest.mark.diagnostics
def test_check_variable_name(common_setup):
    """Test variable name checking and transformations."""
    setup = common_setup
    data = check_variable_name(setup["data"], loglevel=setup["loglevel"])
    # Ensure required variables exist
    assert 'so' in data, "Variable 'so' not found in dataset"
    assert 'thetao' in data, "Variable 'thetao' not found in dataset"
    assert data['thetao'].attrs.get('units') == 'degC', "Units not converted to Celsius"
    assert 'lev' in data.dims, "Dimension 'lev' missing in dataset"

@pytest.fixture
def diagnostics_instances(common_setup):
    """Initialize all diagnostics instances at once."""
    setup = common_setup
    setup["data"] = check_variable_name(setup["data"], loglevel=setup["loglevel"])
    
    return {
        "hovmoller": hovmoller_plot(setup),
        "time_series": time_series(setup),
        "multilevel_trend" : multilevel_trend(setup)
        
        # Add other instances like multilevel_trend, zonal_mean_trend if needed
    }

# Hovmoller Function
@pytest.mark.diagnostics
def test_hovmoller_data(diagnostics_instances):
    """Test data loading for hovmoller plot."""
    hovmoller_instance = diagnostics_instances["hovmoller"]
    hovmoller_instance.data_for_hovmoller_lev_time_plot()
    for i in range(1, len(hovmoller_instance.plot_info) + 1):
        assert hovmoller_instance.plot_info[i]["data"] is not None, f"Data not loaded for hovmoller plot hovmoller_instance.plot_info[1]['type']"
    
    # Check data values
    # Check for the full values
    assert hovmoller_instance.plot_info[1]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-250.51111293,rel=approx_rel)
    assert hovmoller_instance.plot_info[1]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(35.5250391,rel=approx_rel)
        
    # Check for the anomaly wrt initial time
    assert hovmoller_instance.plot_info[3]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-0.38893481,rel=approx_rel)
    assert hovmoller_instance.plot_info[3]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(0.0255231,rel=approx_rel)

    # Check for the Std. anomaly wrt initial time
    assert hovmoller_instance.plot_info[4]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-1.36161743,rel=approx_rel)
    assert hovmoller_instance.plot_info[4]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(0.55796837,rel=approx_rel)
       
# @pytest.mark.diagnostics
# def test_hovmoller_plot(hovmoller_instance):
#     """Test hovmoller plot generation."""
#     hovmoller_instance.plot()
#     output_file = Path(f"{hovmoller_instance.output_dir}/pdf/{hovmoller_instance.model}-{hovmoller_instance.exp}-{hovmoller_instance.source}_hovmoller_plot_indian_ocean.pdf")
#     assert output_file.exists(), "Plot not generated"
    
# #Time_series Function
@pytest.mark.diagnostics
def test_time_series(diagnostics_instances):
    """Test data loading for time series plot."""
    time_series_instance = diagnostics_instances["time_series"]
    time_series_instance.data_for_hovmoller_lev_time_plot()
    for i in range(1, len(time_series_instance.plot_info) + 1):
        assert time_series_instance.plot_info[i]["data"] is not None, f"Data not loaded for hovmoller plot hovmoller_instance.plot_info[1]['type']"
    
    # Check data values
    # Check for the full values
    assert time_series_instance.plot_info[1]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-250.51111292878178,rel=approx_rel)
    assert time_series_instance.plot_info[1]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(35.52503910217593,rel=approx_rel)
        
    # Check for the anomaly wrt initial time
    assert time_series_instance.plot_info[3]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-0.38893480970503447,rel=approx_rel)
    assert time_series_instance.plot_info[3]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(0.02552309870521441,rel=approx_rel)

    # Check for the Std. anomaly wrt initial time
    assert time_series_instance.plot_info[4]['data']['thetao'].isel(time=20, lev = 5).values == pytest.approx(-1.3616174284235687,rel=approx_rel)
    assert time_series_instance.plot_info[4]['data']['so'].isel(time=20, lev = 5).values == pytest.approx(0.5579683747741266,rel=approx_rel)

    
# @pytest.mark.diagnostics
# def test_time_series(time_series_instance):
#     """Test time series plot generation."""
#     time_series_instance.plot()
#     output_file = Path(f"{time_series_instance.output_dir}/pdf/{time_series_instance.model}-{time_series_instance.exp}-{time_series_instance.source}_time_series_indian_ocean.pdf")
#     assert output_file.exists(), "Plot not generated"


# Multilevel trend Function
@pytest.mark.diagnostics
def test_multilevel_trend(diagnostics_instances):
    """Test data loading for time series plot."""
    multilevel_trend_instance = diagnostics_instances["multilevel_trend"]
    trend_dic= multilevel_trend_instance.plot()
    assert trend_dic["trend_data"]["thetao"].isel(lev=1, lat=10, lon=10).values == pytest.approx(0.09392428426243855,rel=approx_rel)
    assert trend_dic["trend_data"]["so"].isel(lev=1, lat=10, lon=10).values == pytest.approx(0.02337264012989936,rel=approx_rel)
    
    
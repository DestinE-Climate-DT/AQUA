import pytest
from aqua import Reader
from ocean3d import check_variable_name

from ocean3d import stratification
from ocean3d import mld

approx_rel = 1e-4

@pytest.fixture
def common_setup(tmp_path):
    """Fixture to set up common configuration and test data."""
    loglevel = 'DEBUG'
    catalog = 'ci'
    exp = 'hpz3'
    model = 'FESOM'
    source = 'monthly-3d'
    region = 'Labrador Sea'
    time = 'DEC'
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
        "region" : 'Labrador Sea',
        "output_dir" : output_dir,
        "output" : output,
        "time": time
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
        "mld": mld(setup),
        "stratification": stratification(setup)
    }

# MLD Function
@pytest.mark.diagnostics
def test_mld(diagnostics_instances):
    """Test data loading for hovmoller plot."""
    mld_instance = diagnostics_instances["mld"]
    mld_dic = mld_instance._process_data()
    # Check data values
    assert mld_dic["mod_clim"]["mld"].isel(lon=10, lat=10).values == pytest.approx(4.52974677,rel=approx_rel)
    
# Stratification Function
@pytest.mark.diagnostics
def test_stratification(diagnostics_instances):
    """Test data loading for hovmoller plot."""
    stratification_instance = diagnostics_instances["stratification"]
    stratification_data = stratification_instance.prepare_data_list()
    # Check data values
    assert stratification_data[0]['rho'].isel(lev=5).values == pytest.approx(-3.31796483e+08,rel=approx_rel)
    
    
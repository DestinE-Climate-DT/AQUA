import os
import pytest
import xarray as xr
from aqua import Reader
from aqua.diagnostics.core import OutputSaver

# Fixture for OutputSaver instance
@pytest.fixture
def output_saver(tmp_path):
    def _factory(**overrides):
        default_args = {
            'diagnostic': 'dummy',
            'model': 'FESOM',
            'exp': 'results',
            'catalog': 'ci',
            'outdir': tmp_path,
            'loglevel': 'DEBUG',
        }
        default_args.update(overrides)
        return OutputSaver(**default_args)
    return _factory

@pytest.fixture
def base_saver(output_saver):
    return output_saver()

@pytest.mark.aqua
def test_save_netcdf(base_saver, tmp_path):
    """Test creating the catalog entry for diagnostics"""
    # Create a simple xarray dataset
    reader = Reader(catalog='ci', model='FESOM', exp='results', source='timeseries1D', areas=False)
    data0 = reader.retrieve(var='2t')
    
    extra_keys = {'var': '2t'}
    base_saver.save_netcdf(dataset=data0, diagnostic_product='test', extra_keys=extra_keys, create_catalog_entry=True)

    reader = Reader(catalog='ci', model='FESOM', exp='results', source='aqua-dummy-test', areas=False)
    
    data1 = reader.retrieve(var='2t')

    assert data0.equals(data1)
    

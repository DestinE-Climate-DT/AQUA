import pytest
import os
import xarray as xr
import matplotlib.pyplot as plt
from aqua.logger import log_configure
from aqua.diagnostics.core import OutputSaver

# Initialize the logger for the tests
log_configure(log_level='DEBUG', log_name='OutputSaverTest')


# Fixture for OutputSaver instance
@pytest.fixture
def output_saver():
    return OutputSaver(diagnostic='dummy', model='IFS-NEMO', exp='historical',
                       catalog='lumi-phase2', loglevel='DEBUG')

@pytest.mark.aqua
def test_generate_name(output_saver):
    """Test the generation of output filenames with and without additional parameters."""
    # Test filename generation without additional parameters
    filename = output_saver.generate_name(diagnostic_product='mean')
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical'

    # Test with generic multimodel keyword
    extra_keys = {'var' : 'tprate'}
    filename = output_saver.generate_name(diagnostic_product='mean', model='multimodel', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multimodel.tprate'

    #Test with multiple references
    extra_keys = {'var': 'tprate', 'region' : 'indian_ocean'}
    filename = output_saver.generate_name(diagnostic_product='mean', model='IFS-NEMO', model_ref=['ERA5', 'CERES'], extra_keys=extra_keys)
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical.multiref.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region' : 'indian_ocean'}
    filename = output_saver.generate_name(diagnostic_product='mean', model=['IFS-NEMO','ICON'], model_ref='ERA5', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multimodel.ERA5.tprate.indian_ocean'


@pytest.mark.aqua
def test_save_netcdf(output_saver):
    """Test saving a netCDF file."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    # Test saving netCDF file 
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean')
    assert os.path.exists(path)

@pytest.mark.aqua
def test_save_png(output_saver, tmpdir):
    """Test saving a PNG file."""

    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])
    
    # Save the PNG file
    extra_keys = {'var' : 'tprate'}
    path = output_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys)
    
    # Check if the file was created
    path = './png/dummy.mean.lumi-phase2.IFS-NEMO.historical.tprate.png'
    assert os.path.exists(path)

@pytest.mark.aqua
def test_save_pdf(output_saver, tmpdir):
    """Test saving a PDF file."""
    # Create a simple figure
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])
    
    # Save the PDF file
    extra_keys = {'var' : 'tprate'}
    output_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys)
    
    # Check if the file was created
    path = './pdf/dummy.mean.lumi-phase2.IFS-NEMO.historical.tprate.pdf'
    assert os.path.exists(path)
    
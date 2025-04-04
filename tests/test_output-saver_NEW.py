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
    return OutputSaver(diagnostic='dummy', model='MSWEP', exp='past',
                       catalog='lumi-phase2', loglevel='DEBUG')

@pytest.mark.aqua
def test_generate_name(output_saver):
    """Test the generation of output filenames with and without additional parameters."""
    # Test filename generation without additional parameters
    filename = output_saver.generate_name(diagnostic_product='mean')
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past'

    # Test with generic multi-model keyword
    extra_keys = {'model': 'multi', 'var' : 'tprate'}
    filename = output_saver.generate_name(diagnostic_product='mean', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multi.tprate'

    # Test with multiple models
    extra_keys = {'model': ['IFS-NEMO', 'ICON', 'ERA5'], 'var': 'tprate', 'region' : 'indian_ocean'}
    filename = output_saver.generate_name(diagnostic_product='mean', extra_keys=extra_keys)
    assert filename == 'dummy.mean.IFS-NEMO_ICON_ERA5.tprate.indian_ocean'

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
    path = './png/dummy.mean.lumi-phase2.MSWEP.past.tprate.png'
    assert os.path.exists(path)

@pytest.mark.aqua
def test_save_pdf(output_saver, tmpdir):
    """Test saving a PDF file."""
    # Create a simple figure
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])
    
    # Save the PDF file
    extra_keys = {'model': 'multi', 'var' : 'tprate'}
    output_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys)
    
    # Check if the file was created
    path = './pdf/dummy.mean.multi.tprate.pdf'
    assert os.path.exists(path)
    
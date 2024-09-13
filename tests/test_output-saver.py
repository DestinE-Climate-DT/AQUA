import os
import pytest
import xarray as xr
import matplotlib.pyplot as plt
from aqua.logger import log_configure
from aqua.util import OutputSaver

# Initialize the logger for the tests
log_configure(log_level='DEBUG', log_name='OutputSaverTest')

# Fixture for OutputSaver instance
@pytest.fixture
def output_saver():
    return OutputSaver(diagnostic='dummy', model='MSWEP', exp='past', catalog='lumi-phase2', loglevel='DEBUG', default_path='.')

@pytest.mark.aqua
def test_generate_name(output_saver):
    """Test the generation of output filenames with and without additional parameters."""
    # Test filename generation without additional parameters
    filename = output_saver.generate_name(diagnostic_product='mean', suffix='nc')
    assert filename == 'dummy.mean.MSWEP.past.lumi-phase2.nc'

    # Test filename generation with a second catalog for comparative studies
    filename = output_saver.generate_name(diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                          time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                          area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert filename == 'dummy.mean.mtpr.MSWEP.past.lumi-phase2.ERA5.era5.lumi-phase3.indian_ocean.199001.199002.frequency_3H.status_preliminary.nc'

@pytest.mark.aqua
def test_save_netcdf(output_saver):
    """Test saving a netCDF file."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    # Test saving netCDF file without additional parameters
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean')
    assert path == './dummy.mean.MSWEP.past.lumi-phase2.nc'

    # Test saving netCDF file with a second catalog for comparative studies
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                    time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                    area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './dummy.mean.mtpr.MSWEP.past.lumi-phase2.ERA5.era5.lumi-phase3.indian_ocean.199001.199002.frequency_3H.status_preliminary.nc'

@pytest.mark.aqua
def test_save_pdf(output_saver):
    """Test saving a PDF file."""
    # Create a simple matplotlib figure
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])

    # Test saving PDF file without additional parameters
    path = output_saver.save_pdf(fig=fig, diagnostic_product='mean')
    assert path == './dummy.mean.MSWEP.past.lumi-phase2.pdf'

    # Test saving PDF file with a second catalog for comparative studies
    path = output_saver.save_pdf(fig=fig, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                 time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                 area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './dummy.mean.mtpr.MSWEP.past.lumi-phase2.ERA5.era5.lumi-phase3.indian_ocean.199001.199002.frequency_3H.status_preliminary.pdf'

@pytest.mark.aqua
def test_save_png(output_saver):
    """Test saving a PNG file."""
    # Create a simple matplotlib figure
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])

    # Test saving PNG file without additional parameters
    path = output_saver.save_png(fig=fig, diagnostic_product='mean')
    assert path == './dummy.mean.MSWEP.past.lumi-phase2.png'

    # Test saving PNG file with a second catalog for comparative studies
    path = output_saver.save_png(fig=fig, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                 time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                 area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './dummy.mean.mtpr.MSWEP.past.lumi-phase2.ERA5.era5.lumi-phase3.indian_ocean.199001.199002.frequency_3H.status_preliminary.png'

@pytest.mark.aqua
def test_missing_diagnostic_product(output_saver):
    """Test that a ValueError is raised when diagnostic_product is not provided."""
    with pytest.raises(ValueError, match="The 'diagnostic_product' parameter is required and cannot be empty."):
        output_saver.generate_name()

    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})
    with pytest.raises(ValueError, match="The 'diagnostic_product' parameter is required and cannot be empty."):
        output_saver.save_netcdf(dataset=data)

    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])
    with pytest.raises(ValueError, match="The 'diagnostic_product' parameter is required and cannot be empty."):
        output_saver.save_pdf(fig=fig)

    with pytest.raises(ValueError, match="The 'diagnostic_product' parameter is required and cannot be empty."):
        output_saver.save_png(fig=fig)

@pytest.mark.aqua
def test_metadata_addition(output_saver):
    """Test saving a file with metadata."""
    # Create a simple xarray dataset and metadata
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})
    metadata = {'author': 'test', 'description': 'test metadata'}

    # Save netCDF file with metadata
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean', metadata=metadata)
    loaded_data = xr.open_dataset(path)
    assert loaded_data.attrs['author'] == 'test'
    assert loaded_data.attrs['description'] == 'test metadata'

    # Create a simple matplotlib figure
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])

    # Save PDF file with metadata
    pdf_path = output_saver.save_pdf(fig=fig, diagnostic_product='mean', metadata=metadata)
    # Check metadata in PDF (this is more complex, requires reading PDF metadata, which is skipped here for simplicity)

@pytest.mark.aqua
def test_overwriting_files(output_saver):
    """Test overwriting files."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    # Save netCDF file with rebuild=True
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean', rebuild=True)
    assert os.path.exists(path)

    # Modify the dataset and save with rebuild=False
    data['data'].values[0, 0] = 100
    output_saver.rebuild = False
    path_no_overwrite = output_saver.save_netcdf(dataset=data, diagnostic_product='mean')
    assert os.path.exists(path_no_overwrite)
    assert xr.open_dataset(path)['data'].values[0, 0] != 100  # Ensure the file was not overwritten

@pytest.mark.aqua
def test_invalid_figure_input(output_saver):
    """Test handling of invalid figure input."""
    # Test saving PDF with invalid figure input
    with pytest.raises(ValueError, match="The provided fig parameter is not a valid matplotlib Figure or pyplot figure."):
        output_saver.save_pdf(fig="invalid_figure", diagnostic_product='mean')

    # Test saving PNG with invalid figure input
    with pytest.raises(ValueError, match="The provided fig parameter is not a valid matplotlib Figure or pyplot figure."):
        output_saver.save_png(fig="invalid_figure", diagnostic_product='mean')

import os
import time
import pytest
import xarray as xr
import matplotlib.pyplot as plt
from aqua.logger import log_configure
from aqua.util import OutputSaver, open_image

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
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past.nc'

    # Test filename generation with a second catalog for comparative studies
    filename = output_saver.generate_name(diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                          time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                          area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past.mtpr.lumi-phase3.ERA5.era5.indian_ocean.199001.199002.ym.frequency_3H.status_preliminary.nc'

    filename = output_saver.generate_name(diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                          time_start='1990-01-01', time_end='1990-02-01', time_precision='y', status="preliminary")
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past.mtpr.ERA5.era5.1990.1990.y.status_preliminary.nc'

    filename = output_saver.generate_name(diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                          time_start='1990-01-01', time_end='1990-02-01',
                                          area='pacific_ocean', catalog_2='lumi-phase3')
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past.mtpr.lumi-phase3.ERA5.era5.pacific_ocean.19900101.19900201.ymd.nc'

    filename = output_saver.generate_name(diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                          time_start='1990-01-01', time_end='1990-02-01', time_precision='ymd')
    assert filename == 'dummy.mean.lumi-phase2.MSWEP.past.mtpr.ERA5.era5.19900101.19900201.ymd.nc'

    output_saver = OutputSaver(diagnostic='tropical_rainfall', model='MSWEP', exp='past', catalog='lumi-phase2',
                               loglevel='DEBUG', default_path='.', filename_keys=['diagnostic', 'catalog', 'model'])
    filename = output_saver.generate_name(var='mtpr', model_2='ERA5', exp_2='era5', time_start='1990-01', time_end='1990-02',
                                          diagnostic_product='mean', time_precision='ym', area='indian_ocean',
                                          frequency="3H", status="preliminary")
    assert filename == 'tropical_rainfall.lumi-phase2.MSWEP.nc'

@pytest.mark.aqua
def test_save_netcdf(output_saver):
    """Test saving a netCDF file."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    # Test saving netCDF file without additional parameters
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean')
    assert path == './netcdf/dummy.mean.lumi-phase2.MSWEP.past.nc'

    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean', path='./')
    assert path == './dummy.mean.lumi-phase2.MSWEP.past.nc'
    
    # Test saving netCDF file with a second catalog for comparative studies
    path = output_saver.save_netcdf(dataset=data, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                    time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                    area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './netcdf/dummy.mean.lumi-phase2.MSWEP.past.mtpr.lumi-phase3.ERA5.era5.indian_ocean.199001.199002.ym.frequency_3H.status_preliminary.nc'

@pytest.mark.aqua
def test_save_pdf(output_saver):
    """Test saving a PDF file."""
    # Create a simple matplotlib figure
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])

    # Test saving PDF file without additional parameters
    path = output_saver.save_pdf(fig=fig, diagnostic_product='mean')
    assert path == './pdf/dummy.mean.lumi-phase2.MSWEP.past.pdf'

    path = output_saver.save_pdf(fig=fig, diagnostic_product='mean', path='./')
    assert path == './dummy.mean.lumi-phase2.MSWEP.past.pdf'

    # Test saving PDF file with a second catalog for comparative studies
    path = output_saver.save_pdf(fig=fig, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                 time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                 area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './pdf/dummy.mean.lumi-phase2.MSWEP.past.mtpr.lumi-phase3.ERA5.era5.indian_ocean.199001.199002.ym.frequency_3H.status_preliminary.pdf'

@pytest.mark.aqua
def test_save_png(output_saver):
    """Test saving a PNG file."""
    # Create a simple matplotlib figure
    fig, ax = plt.subplots()
    ax.plot([1, 2, 3], [4, 5, 6])

    # Test saving PNG file without additional parameters
    path = output_saver.save_png(fig=fig, diagnostic_product='mean')
    assert path == './png/dummy.mean.lumi-phase2.MSWEP.past.png'

    path = output_saver.save_png(fig=fig, diagnostic_product='mean', path='./')
    assert path == './dummy.mean.lumi-phase2.MSWEP.past.png'

    # Test saving PNG file with a second catalog for comparative studies
    path = output_saver.save_png(fig=fig, diagnostic_product='mean', var='mtpr', model_2='ERA5', exp_2='era5',
                                 time_start='1990-01-01', time_end='1990-02-01', time_precision='ym',
                                 area='indian_ocean', catalog_2='lumi-phase3', frequency="3H", status="preliminary")
    assert path == './png/dummy.mean.lumi-phase2.MSWEP.past.mtpr.lumi-phase3.ERA5.era5.indian_ocean.199001.199002.ym.frequency_3H.status_preliminary.png'

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

    # Use the open_image function to open the PDF and verify the metadata
    pdf_metadata = open_image(pdf_path, loglevel='DEBUG')
    assert pdf_metadata.get('description').get('author') == 'test'
    assert pdf_metadata.get('description').get('description') == 'test metadata'

    # Save PNG file with metadata
    png_path = output_saver.save_png(fig=fig, diagnostic_product='mean', metadata=metadata)

    # Use the open_image function to open the PNG and verify the metadata
    png_metadata = open_image(png_path, loglevel='DEBUG')
    assert png_metadata.get('author') == 'test'
    assert png_metadata.get('description') == 'test metadata'

@pytest.mark.aqua
def test_invalid_figure_input(output_saver):
    """Test handling of invalid figure input."""
    # Test saving PDF with invalid figure input
    with pytest.raises(ValueError, match="The provided fig parameter is not a valid matplotlib Figure or pyplot figure."):
        output_saver.save_pdf(fig="invalid_figure", diagnostic_product='mean')

    # Test saving PNG with invalid figure input
    with pytest.raises(ValueError, match="The provided fig parameter is not a valid matplotlib Figure or pyplot figure."):
        output_saver.save_png(fig="invalid_figure", diagnostic_product='mean')

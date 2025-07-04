import pytest
import os
import time
import xarray as xr
from pathlib import Path
import matplotlib.pyplot as plt
from aqua.diagnostics.core import OutputSaver

# Fixture for OutputSaver instance
@pytest.fixture
def output_saver(tmp_path):
    return OutputSaver(diagnostic='dummy', model='IFS-NEMO', exp='historical',
                       catalog='lumi-phase2', outdir=tmp_path, loglevel='DEBUG')

@pytest.mark.aqua
def test_generate_name(output_saver):
    """Test the generation of output filenames with and without additional parameters."""
    # Test filename generation without additional parameters
    filename = output_saver.generate_name(diagnostic_product='mean')
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical'

    # Test with generic multimodel keyword
    extra_keys = {'var': 'tprate'}
    filename = output_saver.generate_name(diagnostic_product='mean', model='multimodel', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multimodel.tprate'

    # Test with multiple references
    extra_keys = {'var': 'tprate', 'region' : 'indian_ocean'}
    filename = output_saver.generate_name(diagnostic_product='mean', model='IFS-NEMO',
                                          model_ref=['ERA5', 'CERES'], extra_keys=extra_keys)
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical.multiref.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region': 'indian_ocean'}
    filename = output_saver.generate_name(diagnostic_product='mean', model=['IFS-NEMO', 'ICON'],
                                          model_ref='ERA5', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multimodel.ERA5.tprate.indian_ocean'


@pytest.mark.aqua
def test_save_netcdf(output_saver, tmp_path):
    """Test saving a netCDF file."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    extra_keys = {'var': 'tprate'}
    output_saver.save_netcdf(dataset=data, diagnostic_product='mean', extra_keys=extra_keys)
    nc = os.path.join(tmp_path, 'netcdf', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.tprate.nc')
    assert os.path.exists(nc)

    old_mtime = Path(nc).stat().st_mtime
    output_saver.save_netcdf(dataset=data, diagnostic_product='mean', rebuild=False)
    new_mtime = Path(nc).stat().st_mtime
    assert new_mtime == old_mtime

@pytest.mark.aqua
def test_save_png(output_saver, tmp_path):
    """Test saving a PNG file."""

    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    # Save the PNG file
    extra_keys = {'var': 'tprate'}
    path = output_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=300)

    # Check if the file was created
    png = os.path.join(tmp_path, 'png', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.tprate.png')
    assert os.path.exists(png)

    old_mtime = Path(png).stat().st_mtime
    output_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=300, rebuild=False)
    new_mtime = Path(png).stat().st_mtime
    assert new_mtime == old_mtime

@pytest.mark.aqua
def test_save_pdf(output_saver, tmp_path):
    """Test saving a PDF file."""
    # Create a simple figure
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    # Save the PDF file
    extra_keys = {'var': 'tprate'}
    output_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys)

    # Check if the file was created
    pdf = os.path.join(tmp_path, 'pdf', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.tprate.pdf')
    assert os.path.exists(pdf)

    old_mtime = Path(pdf).stat().st_mtime
    output_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, rebuild=False)
    new_mtime = Path(pdf).stat().st_mtime
    assert new_mtime == old_mtime

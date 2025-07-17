import os
import pytest
from pathlib import Path
import xarray as xr
import matplotlib.pyplot as plt
from aqua.diagnostics.core import OutputSaver

# Fixture for OutputSaver instance
@pytest.fixture
def output_saver(tmp_path):
    def _factory(**overrides):
        default_args = {
            'diagnostic': 'dummy',
            'model': 'IFS-NEMO',
            'exp': 'historical',
            'catalog': 'lumi-phase2',
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
def test_generate_name(base_saver, output_saver):
    """Test the generation of output filenames with and without additional parameters."""
    # Test filename generation without additional parameters
    
    filename = base_saver.generate_name(diagnostic_product='mean')
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r1'

    # Test with generic multimodel keyword
    extra_keys = {'var': 'tprate'}
    saver = output_saver(model='multimodel')
    filename = saver.generate_name(diagnostic_product='mean', extra_keys=extra_keys)
    assert filename == 'dummy.mean.multimodel.tprate'

    # Test with multiple references
    extra_keys = {'var': 'tprate', 'region' : 'indian_ocean'}
    saver = output_saver(model='IFS-NEMO', realization=2, model_ref=['ERA5', 'CERES'])
    filename = saver.generate_name(
            diagnostic_product='mean', extra_keys=extra_keys
    )
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r2.multiref.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region': 'indian_ocean'}
    saver = output_saver(
        catalog=['lumi-phase2', 'lumi-phase2'], model=['IFS-NEMO', 'ICON'],
        exp=['hist-1990', 'hist-1990'], model_ref='ERA5')
    filename = saver.generate_name(
        diagnostic_product='mean', extra_keys=extra_keys
    )
    assert filename == 'dummy.mean.multimodel.ERA5.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region': 'indian_ocean'}
    saver = output_saver(
        catalog=['lumi-phase2'], model=['IFS-NEMO'],
        exp=['historical'], model_ref=['ERA5'])
    filename = saver.generate_name(
        diagnostic_product='mean', extra_keys=extra_keys
    )
    assert filename == 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r1.ERA5.tprate.indian_ocean'

    with pytest.raises(ValueError):
        # Test with invalid model type
        saver = output_saver(model=['IFS-NEMO', 'ICON'])
        saver.generate_name(diagnostic_product='mean')

    with pytest.raises(ValueError):
        # Test with invalid model type
        saver = output_saver(model=['IFS-NEMO', 'ICON'], catalog=['lumi-phase2', 'lumi-phase2'], exp=['hist-1990'])
        saver.generate_name(diagnostic_product='mean')

@pytest.mark.aqua
def test_internal_function(base_saver):
    """Test internal functions of OutputSaver."""

    # Test cases per _format_realization
    saver = base_saver
    assert saver._format_realization(None) == 'r1'
    assert saver._format_realization('5') == 'r5'
    assert saver._format_realization(5) == 'r5'
    assert saver._format_realization('r5') == 'r5'
    assert saver._format_realization([1, 'r2']) == ['r1', 'r2']

    # Test cases per unpack_list
    assert saver.unpack_list(['item']) == 'item'
    assert saver.unpack_list(['a', 'b']) == ['a', 'b']
    assert saver.unpack_list(None) is None
    assert saver.unpack_list([]) is None

@pytest.mark.aqua
def test_save_netcdf(base_saver, tmp_path):
    """Test saving a netCDF file."""
    # Create a simple xarray dataset
    data = xr.Dataset({'data': (('x', 'y'), [[1, 2], [3, 4]])})

    extra_keys = {'var': 'tprate'}
    base_saver.save_netcdf(dataset=data, diagnostic_product='mean', extra_keys=extra_keys)
    nc = os.path.join(tmp_path, 'netcdf', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r1.tprate.nc')
    assert os.path.exists(nc)

    old_mtime = Path(nc).stat().st_mtime
    base_saver.save_netcdf(dataset=data, diagnostic_product='mean', rebuild=False)
    new_mtime = Path(nc).stat().st_mtime
    assert new_mtime == old_mtime

@pytest.mark.aqua
def test_save_png(base_saver, tmp_path):
    """Test saving a PNG file."""

    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    # Save the PNG file
    extra_keys = {'var': 'tprate'}
    path = base_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=300)

    # Check if the file was created
    png = os.path.join(tmp_path, 'png', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r1.tprate.png')
    assert os.path.exists(png)
    assert path == png

    old_mtime = Path(png).stat().st_mtime
    base_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=300, rebuild=False)
    new_mtime = Path(png).stat().st_mtime
    assert new_mtime == old_mtime

@pytest.mark.aqua
def test_save_pdf(base_saver, tmp_path):
    """Test saving a PDF file."""
    # Create a simple figure
    fig, ax = plt.subplots()
    ax.plot([0, 1], [0, 1])

    # Save the PDF file
    extra_keys = {'var': 'tprate'}
    base_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys)

    # Check if the file was created
    pdf = os.path.join(tmp_path, 'pdf', 'dummy.mean.lumi-phase2.IFS-NEMO.historical.r1.tprate.pdf')
    assert os.path.exists(pdf)

    old_mtime = Path(pdf).stat().st_mtime
    base_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, rebuild=False)
    new_mtime = Path(pdf).stat().st_mtime
    assert new_mtime == old_mtime

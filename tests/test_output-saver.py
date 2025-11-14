import os
import pytest
from pathlib import Path
import xarray as xr
import matplotlib.pyplot as plt
from unittest.mock import patch, MagicMock
from aqua.util import replace_urlpath_jinja, replace_urlpath_wildcard
from aqua.diagnostics.core import OutputSaver
from conftest import DPI, LOGLEVEL

# Fixture for OutputSaver instance
@pytest.fixture
def output_saver(tmp_path):
    def _factory(**overrides):
        default_args = {
            'diagnostic': 'dummy',
            'model': 'IFS-NEMO',
            'exp': 'historical',
            'catalog': 'ci',
            'outputdir': tmp_path,
            'loglevel': LOGLEVEL,
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
    assert filename == 'dummy.mean.ci.IFS-NEMO.historical.r1'

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
    assert filename == 'dummy.mean.ci.IFS-NEMO.historical.r2.multiref.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region': 'indian_ocean'}
    saver = output_saver(
        catalog=['ci', 'ci'], model=['IFS-NEMO', 'ICON'],
        exp=['hist-1990', 'hist-1990'], model_ref='ERA5')
    filename = saver.generate_name(
        diagnostic_product='mean', extra_keys=extra_keys
    )
    assert filename == 'dummy.mean.multimodel.ERA5.tprate.indian_ocean'

    # Test with multiple models
    extra_keys = {'var': 'tprate', 'region': 'indian_ocean'}
    saver = output_saver(
        catalog=['ci'], model=['IFS-NEMO'],
        exp=['historical'], model_ref=['ERA5'])
    filename = saver.generate_name(
        diagnostic_product='mean', extra_keys=extra_keys
    )
    assert filename == 'dummy.mean.ci.IFS-NEMO.historical.r1.ERA5.tprate.indian_ocean'

    with pytest.raises(ValueError):
        # Test with invalid model type
        saver = output_saver(model=['IFS-NEMO', 'ICON'])
        saver.generate_name(diagnostic_product='mean')

    with pytest.raises(ValueError):
        # Test with invalid model type
        saver = output_saver(model=['IFS-NEMO', 'ICON'], catalog=['ci', 'ci'], exp=['hist-1990'])
        saver.generate_name(diagnostic_product='mean')

@pytest.mark.aqua
def test_internal_function(base_saver):
    """Test internal functions of OutputSaver."""
    saver = base_saver

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
    nc = os.path.join(tmp_path, 'netcdf', 'dummy.mean.ci.IFS-NEMO.historical.r1.tprate.nc')
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
    path = base_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=DPI)

    # Check if the file was created
    png = os.path.join(tmp_path, 'png', 'dummy.mean.ci.IFS-NEMO.historical.r1.tprate.png')
    assert os.path.exists(png)
    assert path == png

    old_mtime = Path(png).stat().st_mtime
    base_saver.save_png(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, dpi=DPI, rebuild=False)
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
    pdf = os.path.join(tmp_path, 'pdf', 'dummy.mean.ci.IFS-NEMO.historical.r1.tprate.pdf')
    assert os.path.exists(pdf)

    old_mtime = Path(pdf).stat().st_mtime
    base_saver.save_pdf(fig=fig, diagnostic_product='mean', extra_keys=extra_keys, rebuild=False)
    new_mtime = Path(pdf).stat().st_mtime
    assert new_mtime == old_mtime

@pytest.mark.aqua
def test_create_catalog_entry_new_entry(base_saver, tmp_path):
    """Test creating a new catalog entry when none exists."""
    
    mock_config_path = MagicMock()
    mock_config_path.configdir = str(tmp_path)
    mock_catalog_file = {'sources': {}}
    
    with patch('aqua.diagnostics.core.output_saver.ConfigPath', return_value=mock_config_path), \
         patch('aqua.diagnostics.core.output_saver.load_yaml', return_value=mock_catalog_file), \
         patch('aqua.diagnostics.core.output_saver.dump_yaml') as mock_dump_yaml, \
         patch('aqua.diagnostics.core.output_saver.replace_intake_vars', return_value='/mocked/path/data.nc'):
        
        (tmp_path / 'catalogs' / 'ci' / 'catalog' / 'IFS-NEMO').mkdir(parents=True, exist_ok=True)

        result = base_saver._create_catalog_entry('/test/path/data.nc', {'diagnostic_product': 'mean'})

        assert result['driver'] == 'netcdf'
        assert result['args']['urlpath'] == '/mocked/path/data.nc'
        mock_dump_yaml.assert_called_once()

@pytest.mark.aqua
def test_create_catalog_entry_existing_entry(base_saver, tmp_path, monkeypatch):
    """Test updating an existing catalog entry."""

    mock_config_path = MagicMock()
    mock_config_path.configdir = str(tmp_path)
    existing_catblock = {
        'driver': 'netcdf', 'description': 'Existing', 'args': {'urlpath': '/old/path/data.nc', 'chunks': {'time': 1}},
        'metadata': {'source_grid_name': 'lon-lat'}
    }
    mock_catalog_file = {'sources': {'aqua-dummy-mean': existing_catblock}}
    
    with patch('aqua.diagnostics.core.output_saver.ConfigPath', return_value=mock_config_path), \
         patch('aqua.diagnostics.core.output_saver.load_yaml', return_value=mock_catalog_file), \
         patch('aqua.diagnostics.core.output_saver.dump_yaml') as mock_dump_yaml, \
         patch('aqua.diagnostics.core.output_saver.replace_intake_vars', return_value='/new/path/data.nc'):
        
        (tmp_path / 'catalogs' / 'ci' / 'catalog' / 'IFS-NEMO').mkdir(parents=True, exist_ok=True)
        
        result = base_saver._create_catalog_entry('/test/path/data.nc', {'diagnostic_product': 'mean'})
        
        assert result['args']['urlpath'] == '/new/path/data.nc'
        assert result['args']['xarray_kwargs']['decode_times'] is True
        assert result['args']['chunks'] == {'time': 1}
        mock_dump_yaml.assert_called_once()

@pytest.mark.aqua
def test_create_catalog_entry_with_variables(base_saver, tmp_path):
    """Test creating catalog entry with jinja and wildcard variable replacements."""
    
    mock_config_path = MagicMock()
    mock_config_path.configdir = str(tmp_path)
    mock_catalog_file = {'sources': {}}
    
    with patch('aqua.diagnostics.core.output_saver.ConfigPath', return_value=mock_config_path), \
         patch('aqua.diagnostics.core.output_saver.load_yaml', return_value=mock_catalog_file), \
         patch('aqua.diagnostics.core.output_saver.dump_yaml') as mock_dump_yaml, \
         patch('aqua.diagnostics.core.output_saver.replace_urlpath_jinja') as mock_replace_jinja, \
         patch('aqua.diagnostics.core.output_saver.replace_urlpath_wildcard') as mock_replace_wildcard, \
         patch('aqua.diagnostics.core.output_saver.replace_intake_vars', return_value='/mocked/path/data.nc'):

        (tmp_path / 'catalogs' / 'ci' / 'catalog' / 'IFS-NEMO').mkdir(parents=True, exist_ok=True)
        
        metadata = {'diagnostic_product': 'mean', 'region': 'global', 'realization': 'r1'}
        filepath = '/test/path/data.nc'
        
        # Mock return values
        mock_replace_jinja.return_value = {'driver': 'netcdf', 'description': 'AQUA dummy data for mean',
                                          'args': {'urlpath': '/mocked/path/data.nc', 'chunks': {}},
                                          'metadata': {'source_grid_name': False}}
        mock_replace_wildcard.return_value = mock_replace_jinja.return_value
        
        _ = base_saver._create_catalog_entry(filepath, metadata, 
                                                jinjalist=['region'], wildcardlist=['realization'])
        # Verify replacements were called
        assert mock_replace_jinja.call_count == 1
        assert mock_replace_wildcard.call_count == 1
        mock_dump_yaml.assert_called_once()

@pytest.mark.aqua
def test_create_catalog_entry_edge_cases(base_saver, tmp_path):
    """Test edge cases: None metadata values, file operations, and entry naming."""
    
    mock_config_path = MagicMock()
    mock_config_path.configdir = str(tmp_path)
    mock_catalog_file = {'sources': {}}
    
    with patch('aqua.diagnostics.core.output_saver.ConfigPath', return_value=mock_config_path), \
         patch('aqua.diagnostics.core.output_saver.load_yaml', return_value=mock_catalog_file), \
         patch('aqua.diagnostics.core.output_saver.dump_yaml') as mock_dump_yaml, \
         patch('aqua.diagnostics.core.output_saver.replace_intake_vars', return_value='/mocked/path/data.nc'):

        (tmp_path / 'catalogs' / 'ci' / 'catalog' / 'IFS-NEMO').mkdir(parents=True, exist_ok=True)
        
        # Test None metadata values
        metadata = {'diagnostic_product': 'mean', 'region': None, 'stat': None}
        result = base_saver._create_catalog_entry('/test/path/data.nc', metadata, 
                                                jinjalist=['region'], wildcardlist=['stat'])
        assert result['driver'] == 'netcdf'
        
        # Test entry naming for different products
        for product, expected_name in [('mean', 'aqua-dummy-mean'), ('std', 'aqua-dummy-std')]:
            metadata = {'diagnostic_product': product}
            result = base_saver._create_catalog_entry('/test/path/data.nc', metadata)
            assert expected_name in mock_catalog_file['sources']
        
        # Verify file operations
        mock_dump_yaml.assert_called()
        call_args = mock_dump_yaml.call_args
        updated_catalog = call_args[1]['cfg']
        assert 'aqua-dummy-mean' in updated_catalog['sources']

@pytest.mark.aqua
def test_replace_urlpath_wildcard():
    """Test wildcard replacement in URL paths."""
    
    # Test that replacement only happens when surrounded by same character
    block = {'args': {'urlpath': 'data_r1_data.nc'}}
    result = replace_urlpath_wildcard(block, 'r1')
    assert result['args']['urlpath'] == 'data_*_data.nc'
    
    # Test no replacement when not surrounded by same character
    block = {'args': {'urlpath': '/path/to/r1_data.nc'}}
    result = replace_urlpath_wildcard(block, 'r1')
    assert result['args']['urlpath'] == '/path/to/r1_data.nc'
    
    # Test edge cases
    assert replace_urlpath_wildcard(block, None) == block
    assert replace_urlpath_wildcard(block, '') == block

@pytest.mark.aqua
def test_replace_urlpath_jinja():
    """Test Jinja template replacement and parameter management."""
    
    # Test URL replacement when surrounded by same character
    block = {'args': {'urlpath': 'data_global_data.nc'}}
    result = replace_urlpath_jinja(block, 'global', 'region')
    assert result['args']['urlpath'] == 'data_{{region}}_data.nc'
    
    # Test parameters block creation
    assert result['parameters']['region']['default'] == 'global'
    assert result['parameters']['region']['allowed'] == ['global']
    
    # Test adding second value
    result = replace_urlpath_jinja(result, 'europe', 'region')
    assert 'europe' in result['parameters']['region']['allowed']

@pytest.mark.aqua
def test_core_save(base_saver, tmp_path):
    """Test core save functionality."""
    result = base_saver._core_save('mean', 'pdf')
    assert result.endswith('.pdf')
    
    with pytest.raises(ValueError, match="file_format must be either 'pdf',  'png' or 'nc'"):
        base_saver._core_save('mean', 'txt')

@pytest.mark.aqua
def test_generate_folder(base_saver, tmp_path):
    """Test folder generation."""
    result = base_saver.generate_folder('pdf')
    assert result == str(tmp_path / 'pdf')
    assert os.path.exists(result)

@pytest.mark.aqua
def test_generate_path(base_saver, tmp_path):
    """Test full path generation."""
    result = base_saver.generate_path('pdf', 'mean')
    expected = os.path.join(str(tmp_path / 'pdf'), 'dummy.mean.ci.IFS-NEMO.historical.r1.pdf')
    assert result == expected

@pytest.mark.aqua
def test_create_metadata(base_saver):
    """Test metadata creation and merging."""
    result = base_saver.create_metadata('mean')
    assert result['diagnostic'] == 'dummy'
    assert result['catalog'] == 'ci'
    
    # Test with extra keys and list handling
    extra_keys = {'var': ['tprate', 'temp'], 'region': 'global'}
    result = base_saver.create_metadata('mean', extra_keys=extra_keys)
    assert result['var'] == 'tprate,temp'
    assert result['region'] == 'global'

@pytest.mark.aqua
def test_verify_arguments(base_saver):
    """Test argument validation logic."""
    assert base_saver._verify_arguments(['catalog', 'model', 'exp']) is True
    
    # Test list validation
    saver = base_saver
    saver.catalog, saver.model, saver.exp = ['cat1', 'cat2'], ['mod1', 'mod2'], ['exp1', 'exp2']
    assert saver._verify_arguments(['catalog', 'model', 'exp']) is True
    
    # Test mixed types error
    saver.catalog = 'single'
    with pytest.raises(ValueError, match="must be either all strings or all lists"):
        saver._verify_arguments(['catalog', 'model', 'exp'])

@pytest.mark.aqua
def test_save_netcdf_with_catalog(base_saver, tmp_path, monkeypatch):
    """Test saving NetCDF with catalog entry creation."""
    from unittest.mock import patch
    data = xr.Dataset({'data': (('x',), [1, 2, 3])})
    
    with patch.object(base_saver, '_create_catalog_entry') as mock_create_catalog:
        result = base_saver.save_netcdf(
            dataset=data, diagnostic_product='mean', create_catalog_entry=True,
            dict_catalog_entry={'jinjalist': ['region'], 'wildcardlist': ['var']})
        
        mock_create_catalog.assert_called_once()
        call_args = mock_create_catalog.call_args[1]
        assert call_args['jinjalist'] == ['region']
        assert call_args['wildcardlist'] == ['var']
        assert os.path.exists(result)
"""Tests for DiagnosticCLI base class."""
from types import SimpleNamespace
import os
import pytest

from aqua.diagnostics.core.cli_base import DiagnosticCLI
from aqua.util import dump_yaml


@pytest.fixture
def mock_config_yaml(tmp_path):
    """Create a minimal mock YAML config file for testing."""
    config = {
        'datasets': [
            {
                'catalog': 'test-catalog',
                'model': 'TestModel',
                'exp': 'test-exp',
                'source': 'test-source',
            }
        ],
        'references': [
            {
                'catalog': 'ref-catalog',
                'model': 'RefModel',
                'exp': 'ref-exp',
                'source': 'ref-source',
            }
        ],
        'output': {
            'outputdir': os.path.join(str(tmp_path), 'output'),
            'rebuild': False,
            'save_pdf': True,
            'save_png': False,
            'save_netcdf': True,
            'dpi': 150,
            'create_catalog_entry': True
        }
    }
    
    config_file = os.path.join(str(tmp_path), "test_config.yaml")
    dump_yaml(outfile=config_file, cfg=config)
    return config_file


@pytest.fixture
def mock_args():
    """Create mock command-line arguments."""
    return SimpleNamespace(
        loglevel='INFO',
        catalog=None,
        model=None,
        exp=None,
        source=None,
        realization=None,
        config=None,
        nworkers=None,
        cluster=None,
        regrid=None,
        outputdir=None,
        startdate=None,
        enddate=None
    )


@pytest.mark.aqua
class TestDiagnosticCLI:
    """Tests for DiagnosticCLI class."""

    def test_init_creates_instance(self, mock_args):
        """Test that DiagnosticCLI can be instantiated."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        assert cli.args == mock_args
        assert cli.diagnostic_name == 'test_diagnostic'
        assert cli.default_config == 'config_test.yaml'
        assert cli.log_name == 'Test_diagnostic CLI'
        assert cli.logger is None  # Not initialized yet

    def test_setup_logging(self, mock_args):
        """Test that _setup_logging initializes logger correctly."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        
        assert cli.logger is not None
        assert cli.loglevel == 'INFO'

    def test_extract_options_from_config(self, mock_args, mock_config_yaml, tmp_path):
        """Test that _extract_options correctly extracts settings from config."""
        mock_args.config = str(mock_config_yaml)
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        # Setup logging first (required by _load_config)
        cli._setup_logging()
        
        # Load config and extract options
        cli._load_config()
        cli._extract_options()
        
        # Verify extracted options match the mock config
        assert cli.outputdir == str(tmp_path / 'output')
        assert cli.rebuild is False
        assert cli.save_pdf is True
        assert cli.save_png is False
        assert cli.save_netcdf is True
        assert cli.dpi == 150
        assert cli.create_catalog_entry is True
        assert cli.reader_kwargs == {}

    def test_extract_options_with_realization(self, mock_args, mock_config_yaml):
        """Test that realization is correctly handled."""
        mock_args.config = str(mock_config_yaml)
        mock_args.realization = 'r1i1p1f1'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        cli._extract_options()
        
        assert cli.realization == 'r1i1p1f1'
        assert cli.reader_kwargs == {'realization': 'r1i1p1f1'}

    def test_dataset_args_returns_correct_mapping(self, mock_args):
        """Test that dataset_args extracts correct dataset arguments."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        # Set a regrid value
        cli.regrid = 'r100'
        
        dataset = {
            'catalog': 'my-catalog',
            'model': 'MyModel',
            'exp': 'historical',
            'source': 'CMIP6',
            'startdate': '2000-01-01',
            'enddate': '2010-12-31'
        }
        
        result = cli.dataset_args(dataset)
        
        assert result['catalog'] == 'my-catalog'
        assert result['model'] == 'MyModel'
        assert result['exp'] == 'historical'
        assert result['source'] == 'CMIP6'
        assert result['regrid'] == 'r100'
        assert result['startdate'] == '2000-01-01'
        assert result['enddate'] == '2010-12-31'

    def test_dataset_args_uses_defaults(self, mock_args):
        """Test that dataset_args uses defaults for missing keys."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli.regrid = 'r200'
        
        dataset = {
            'catalog': 'my-catalog',
            'model': 'MyModel',
            'exp': 'historical',
            'source': 'CMIP6'
        }
        
        result = cli.dataset_args(dataset)
        
        assert result['regrid'] == 'r200'  # From cli.regrid
        assert result['startdate'] is None
        assert result['enddate'] is None

    def test_prepare_method_chaining(self, mock_args, mock_config_yaml):
        """Test that prepare() enables method chaining."""
        mock_args.config = str(mock_config_yaml)
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        result = cli.prepare()
        
        assert result is cli
        assert cli.logger is not None
        assert cli.config_dict is not None

    def test_prepare_with_overrides(self, mock_args, mock_config_yaml):
        """Test that prepare() applies overrides correctly."""
        mock_args.config = str(mock_config_yaml)
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli.prepare(save_pdf=False, dpi=600)
        
        assert cli.save_pdf is False  # Overridden
        assert cli.dpi == 600  # Overridden
        assert cli.save_netcdf is True  # Not overridden, from config

    def test_custom_log_name(self, mock_args):
        """Test that custom log_name is used."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml',
            log_name='CustomLogger'
        )
        
        assert cli.log_name == 'CustomLogger'

    def test_extract_options_with_regrid_from_args(self, mock_args, mock_config_yaml):
        """Test that regrid from CLI args is correctly extracted."""
        mock_args.config = str(mock_config_yaml)
        mock_args.regrid = 'r250'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        cli._extract_options()
        
        assert cli.regrid == 'r250'

    def test_extract_options_with_outputdir_from_args(self, mock_args, mock_config_yaml, tmp_path):
        """Test that outputdir from CLI args overrides config."""
        mock_args.config = str(mock_config_yaml)
        custom_output = str(tmp_path / 'custom_output')
        mock_args.outputdir = custom_output
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        cli._extract_options()
        
        # The outputdir from args should override the config
        assert cli.outputdir == custom_output

    def test_dataset_args_with_regrid_from_args(self, mock_args, mock_config_yaml):
        """Test that dataset_args uses regrid from CLI args."""
        mock_args.config = str(mock_config_yaml)
        mock_args.regrid = 'r300'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        cli._extract_options()
        
        dataset = {
            'catalog': 'test-catalog',
            'model': 'TestModel',
            'exp': 'test-exp',
            'source': 'test-source'
        }
        
        result = cli.dataset_args(dataset)
        
        # Should use regrid from args
        assert result['regrid'] == 'r300'

    def test_dataset_args_with_startdate_enddate(self, mock_args):
        """Test that dataset_args correctly handles startdate and enddate."""
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        dataset = {
            'catalog': 'test-catalog',
            'model': 'TestModel',
            'exp': 'test-exp',
            'source': 'test-source',
            'startdate': '2015-01-01',
            'enddate': '2020-12-31'
        }
        
        result = cli.dataset_args(dataset)
        
        assert result['startdate'] == '2015-01-01'
        assert result['enddate'] == '2020-12-31'

    def test_full_workflow_with_cli_args(self, mock_args, mock_config_yaml, tmp_path):
        """Test complete workflow with various CLI arguments passed."""
        # Setup CLI args with specific values
        mock_args.config = str(mock_config_yaml)
        mock_args.regrid = 'r400'
        mock_args.realization = 'r2i1p1f1'
        mock_args.outputdir = str(tmp_path / 'test_output')
        mock_args.loglevel = 'DEBUG'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        # Run the full prepare workflow
        cli.prepare()
        
        # Verify all args are correctly applied
        assert cli.loglevel == 'DEBUG'
        assert cli.regrid == 'r400'
        assert cli.realization == 'r2i1p1f1'
        assert cli.reader_kwargs == {'realization': 'r2i1p1f1'}
        assert cli.outputdir == str(tmp_path / 'test_output')
        
        # Test dataset_args with the prepared CLI
        dataset = {
            'catalog': 'test-catalog',
            'model': 'TestModel',
            'exp': 'test-exp',
            'source': 'test-source',
            'startdate': '2018-06-01',
            'enddate': '2019-05-31'
        }
        
        result = cli.dataset_args(dataset)
        
        assert result['regrid'] == 'r400'  # From args
        assert result['startdate'] == '2018-06-01'
        assert result['enddate'] == '2019-05-31'

    def test_dataset_regrid_override(self, mock_args, mock_config_yaml):
        """Test that dataset-specific regrid overrides CLI regrid."""
        mock_args.config = str(mock_config_yaml)
        mock_args.regrid = 'r100'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        cli._extract_options()
        
        # Dataset with its own regrid specification
        dataset = {
            'catalog': 'test-catalog',
            'model': 'TestModel',
            'exp': 'test-exp',
            'source': 'test-source',
            'regrid': 'r500'  # Dataset-specific regrid
        }
        
        result = cli.dataset_args(dataset)
        
        # Dataset regrid should override CLI regrid
        assert result['regrid'] == 'r500'

    def test_merge_config_with_cli_args(self, mock_args, mock_config_yaml):
        """Test that CLI args override first dataset in config."""
        mock_args.config = str(mock_config_yaml)
        mock_args.catalog = 'override-catalog'
        mock_args.model = 'OverrideModel'
        mock_args.exp = 'override-exp'
        mock_args.source = 'override-source'
        
        cli = DiagnosticCLI(
            args=mock_args,
            diagnostic_name='test_diagnostic',
            default_config='config_test.yaml'
        )
        
        cli._setup_logging()
        cli._load_config()
        
        # Check that first dataset in config was overridden
        first_dataset = cli.config_dict['datasets'][0]
        assert first_dataset['catalog'] == 'override-catalog'
        assert first_dataset['model'] == 'OverrideModel'
        assert first_dataset['exp'] == 'override-exp'
        assert first_dataset['source'] == 'override-source'

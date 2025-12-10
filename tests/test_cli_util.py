"""Test for CLI utility functions"""

import pytest
import argparse
from aqua.core.util import template_parse_arguments

pytestmark = pytest.mark.aqua


def test_template_parse_arguments():
    """Test that template_parse_arguments adds all expected arguments."""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    
    # Parse with all arguments
    args = parser.parse_args([
        '--loglevel', 'INFO',
        '--catalog', 'test_catalog',
        '--model', 'IFS',
        '--exp', 'test-exp',
        '--source', 'monthly',
        '--realization', 'r1',
        '--config', 'config.yaml',
        '--nworkers', '2',
        '--cluster', 'tcp://127.0.0.1:8786',
        '--regrid', 'r100',
        '--outputdir', '/tmp/output',
        '--startdate', '2020-01-01',
        '--enddate', '2020-12-31'
    ])
    
    assert args.loglevel == 'INFO'
    assert args.catalog == 'test_catalog'
    assert args.model == 'IFS'
    assert args.exp == 'test-exp'
    assert args.source == 'monthly'
    assert args.realization == 'r1'
    assert args.config == 'config.yaml'
    assert args.nworkers == 2
    assert args.cluster == 'tcp://127.0.0.1:8786'
    assert args.regrid == 'r100'
    assert args.outputdir == '/tmp/output'
    assert args.startdate == '2020-01-01'
    assert args.enddate == '2020-12-31'


def test_template_parse_arguments_optional():
    """Test that all arguments are optional."""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    
    # Parse with no arguments - should not raise an error
    args = parser.parse_args([])
    
    assert args.loglevel is None
    assert args.catalog is None
    assert args.model is None
    assert args.exp is None
    assert args.source is None
    assert args.realization is None
    assert args.config is None
    assert args.nworkers is None
    assert args.cluster is None
    assert args.regrid is None
    assert args.outputdir is None
    assert args.startdate is None
    assert args.enddate is None


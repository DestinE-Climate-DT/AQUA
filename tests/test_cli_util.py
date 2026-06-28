"""Test for CLI utility functions"""

import argparse

import pytest

from aqua.core.console.drop import drop_parser
from aqua.core.util import template_parse_arguments

pytestmark = pytest.mark.aqua


def test_template_parse_arguments():
    """Test that template_parse_arguments adds all expected arguments."""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)

    # fmt: off
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
    # fmt: on
    assert args.loglevel == "INFO"
    assert args.catalog == "test_catalog"
    assert args.model == "IFS"
    assert args.exp == "test-exp"
    assert args.source == "monthly"
    assert args.realization == "r1"
    assert args.config == "config.yaml"
    assert args.nworkers == 2
    assert args.cluster == "tcp://127.0.0.1:8786"
    assert args.regrid == "r100"
    assert args.outputdir == "/tmp/output"
    assert args.startdate == "2020-01-01"
    assert args.enddate == "2020-12-31"


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


class TestDROPParserLevel:
    """Tests for DROP CLI parser level argument parsing."""

    @pytest.mark.parametrize(
        "level_arg,expected",
        [
            (["--level", "1000"], "1000"),
            (["--level", "1000,850,500"], "1000,850,500"),
            (["--level", "1000.5"], "1000.5"),
            (["--level", "1000.5,850.7,500.2"], "1000.5,850.7,500.2"),
            (["--level", "1000,850.5,500"], "1000,850.5,500"),
            ([], None),
        ],
        ids=[
            "single_int",
            "multiple_int",
            "single_float",
            "multiple_float",
            "mixed_types",
            "no_level",
        ],
    )
    def test_drop_parser_level_argument(self, level_arg, expected):
        """Test DROP parser correctly parses level argument in various formats."""
        parser = drop_parser()
        args = parser.parse_args(level_arg)
        assert args.level == expected

    @pytest.mark.parametrize(
        "level_str,expected_list",
        [
            ("1000", [1000]),
            ("1000,850,500", [1000, 850, 500]),
            ("1000.5", [1000.5]),
            ("1000.5,850.7", [1000.5, 850.7]),
            ("1000,850.5,500", [1000, 850.5, 500]),
        ],
        ids=[
            "single_int",
            "multiple_int",
            "single_float",
            "multiple_float",
            "mixed_types",
        ],
    )
    def test_drop_level_string_to_list_conversion(self, level_str, expected_list):
        """Test conversion logic for comma-separated level strings to properly typed lists."""
        # This mimics the conversion logic in drop_execute
        result = [float(lev) if "." in lev else int(lev) for lev in level_str.split(",")]
        assert result == expected_list

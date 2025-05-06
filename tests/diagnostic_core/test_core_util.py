import pytest
import argparse
import pandas as pd
from unittest.mock import patch
from aqua import Reader
from aqua.diagnostics.core import template_parse_arguments, load_diagnostic_config
from aqua.diagnostics.core import open_cluster, close_cluster, merge_config_args
from aqua.diagnostics.core import convert_data_units, start_end_dates

loglevel = 'DEBUG'


@pytest.mark.aqua
def test_template_parse_arguments():
    """Test the template_parse_arguments function"""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    args = parser.parse_args(["--loglevel", "DEBUG", "--catalog", "test_catalog", "--model", "test_model",
                              "--exp", "test_exp", "--source", "test_source", "--config", "test_config.yaml",
                              "--regrid", "r100", "--outputdir", "test_outputdir", "--cluster", "test_cluster",
                              "--nworkers", "2"])

    assert args.loglevel == "DEBUG"
    assert args.catalog == "test_catalog"
    assert args.model == "test_model"
    assert args.exp == "test_exp"
    assert args.source == "test_source"
    assert args.config == "test_config.yaml"
    assert args.regrid == "r100"
    assert args.outputdir == "test_outputdir"
    assert args.cluster == "test_cluster"
    assert args.nworkers == 2

    with pytest.raises(ValueError):
        load_diagnostic_config(diagnostic='pippo', args=args, loglevel=loglevel)

@pytest.mark.aqua
@patch("aqua.diagnostics.core.util.Client")
@patch("aqua.diagnostics.core.util.LocalCluster")
def test_cluster(mock_cluster, mock_client):
    """Test the cluster functions with mocking"""

    # Test case 1: No workers specified
    client, cluster, private_cluster = open_cluster(None, None, loglevel)
    assert client is None
    assert cluster is None
    assert private_cluster is False

    # # Test case 2: New cluster creation
    client, cluster, private_cluster = open_cluster(2, None, loglevel)
    assert client is not None
    assert cluster is not None
    assert private_cluster is True

    # Test case 3: Using existing cluster
    previous_cluster = mock_cluster
    client, cluster, private_cluster = open_cluster(2, previous_cluster, loglevel)
    assert client is not None
    assert cluster is previous_cluster
    assert private_cluster is False

    close_cluster(client, cluster, private_cluster)


@pytest.mark.aqua
def test_load_diagnostic_config():
    """Test the load_diagnostic_config function"""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    args = parser.parse_args(["--loglevel", "DEBUG"])
    ts_dict = load_diagnostic_config(diagnostic='timeseries',
                                     default_config='config_timeseries_atm.yaml',
                                     args=args, loglevel=loglevel)

    assert ts_dict['datasets'] == [{'catalog': None, 'exp': None, 'model': None, 'source': 'lra-r100-monthly', 'regrid': None}]


@pytest.mark.aqua
def test_merge_config_args():
    """Test the merge_config_args function"""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    args = parser.parse_args(["--loglevel", "DEBUG", "--catalog", "test_catalog", "--model", "test_model",
                              "--exp", "test_exp", "--source", "test_source", "--outputdir", "test_outputdir"])

    ts_dict = {'datasets': [{'catalog': None, 'model': None, 'exp': None, 'source': 'lra-r100-monthly'}],
               'references': [{'catalog': 'obs', 'model': 'ERA5', 'exp': 'era5', 'source': 'monthly'}],
               'output': {'outputdir': './'}}

    merged_config = merge_config_args(config=ts_dict, args=args, loglevel=loglevel)

    assert merged_config['datasets'] == [{'catalog': 'test_catalog', 'exp': 'test_exp',
                                          'model': 'test_model', 'source': 'test_source'}]
    assert merged_config['output']['outputdir'] == 'test_outputdir'


@pytest.mark.aqua
def test_convert_data_units():
    """Test the check_data function"""
    data = Reader(catalog='ci', model='ERA5', exp='era5-hpz3', source='monthly', loglevel=loglevel).retrieve()
    initial_units = data['tprate'].attrs['units']

    # Dataset test
    data_test = convert_data_units(data=data, var='tprate', units='mm/day', loglevel=loglevel)
    assert data_test['tprate'].attrs['units'] == 'mm/day'

    # DataArray test
    data = data['tprate']
    data_test = convert_data_units(data=data, var='tprate', units='mm/day', loglevel=loglevel)
    # We don't test values since this is done in the test_fixer.py
    assert data_test.attrs['units'] == 'mm/day'
    assert f"Converting units of tprate: from {initial_units} to mm/day" in data_test.attrs['history']

    # Test with no conversion to be done
    data_test = convert_data_units(data=data, var='tprate', units=initial_units, loglevel=loglevel)
    assert data_test.attrs['units'] == initial_units
    assert f"Converting units of tprate: from {initial_units} to mm/day" not in data_test.attrs['history']


@pytest.mark.aqua
def test_start_end_dates():
    # All None inputs
    assert start_end_dates() == (None, None)

    # Only startdate provided
    assert start_end_dates(startdate="2020-01-01") == (pd.Timestamp("2020-01-01"), None)

    # Two dates provided
    assert start_end_dates(startdate="2020-01-01", enddate="2020-01-02") == (
        pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")
    )
    assert start_end_dates(startdate="20200101", enddate="20200102") == (
        pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")
    )
    assert start_end_dates(startdate="20200101", start_std="20200102") == (
        pd.Timestamp("2020-01-01"), None
    )
    assert start_end_dates(startdate="2020-01-01", enddate="20200102") == (
        pd.Timestamp("2020-01-01"), pd.Timestamp("2020-01-02")
    )

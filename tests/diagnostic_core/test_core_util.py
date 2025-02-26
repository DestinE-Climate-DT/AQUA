import pytest
import argparse
from unittest.mock import patch
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.core import start_end_dates

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
def test_start_end_dates():
    # All None inputs
    assert start_end_dates() == (None, None)
    
    # Only startdate provided
    assert start_end_dates(startdate="2020-01-01") == ("2020-01-01", None)
    assert start_end_dates(startdate="20200101") == ("20200101", None)

    # Two dates provided
    assert start_end_dates(startdate="2020-01-01", enddate="2020-01-02") == ("2020-01-01", "2020-01-02")
    assert start_end_dates(startdate="20200101", enddate="20200102") == ("20200101", "20200102")
    assert start_end_dates(startdate="20200101", start_std="20200102") == ("20200101", None)
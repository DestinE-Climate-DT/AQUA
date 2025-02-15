import pytest
import argparse
from unittest.mock import patch
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster

loglevel = 'DEBUG'


@pytest.mark.aqua
def test_template_parse_arguments():
    """Test the template_parse_arguments function"""
    parser = argparse.ArgumentParser()
    parser = template_parse_arguments(parser)
    args = parser.parse_args(["--loglevel", "DEBUG", "--catalog", "test_catalog", "--model", "test_model"])

    assert args.loglevel == "DEBUG"
    assert args.catalog == "test_catalog"
    assert args.model == "test_model"
    assert args.exp is None
    assert args.source is None
    assert args.config is None


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

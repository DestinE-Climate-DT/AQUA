"""
Utility functions for the CLI
"""
import argparse
import os
from dask.distributed import Client, LocalCluster
from aqua.logger import log_configure
from aqua.util import load_yaml, get_arg
from aqua.util import ConfigPath


def template_parse_arguments(parser: argparse.ArgumentParser):
    """
    Add the default arguments to the parser.

    Args:
        parser: argparse.ArgumentParser

    Returns:
        argparse.ArgumentParser
    """
    parser.add_argument("--loglevel", "-l", type=str,
                        required=False, help="loglevel")
    parser.add_argument("--catalog", type=str,
                        required=False, help="catalog name")
    parser.add_argument("--model", type=str,
                        required=False, help="model name")
    parser.add_argument("--exp", type=str,
                        required=False, help="experiment name")
    parser.add_argument("--source", type=str,
                        required=False, help="source name")
    parser.add_argument("--config", "-c", type=str,
                        help='yaml configuration file')
    parser.add_argument("--nworkers", "-n", type=int,
                        required=False, help="number of workers")
    parser.add_argument("--cluster", type=str,
                        required=False, help="cluster address")
    parser.add_argument("--regrid", type=str,
                        required=False, help="target regrid resolution")
    parser.add_argument("--outputdir", type=str,
                        required=False, help="output directory")

    return parser


def open_cluster(nworkers, cluster, loglevel: str = 'WARNING'):
    """
    Open a dask cluster if nworkers is provided, otherwise connect to an existing cluster.

    Args:
        nworkers (int): number of workers
        cluster (str): cluster address
        loglevel (str): logging level

    Returns:
        client (dask.distributed.Client): dask client
        cluster (dask.distributed.LocalCluster): dask cluster
        private_cluster (bool): whether the cluster is private
    """

    logger = log_configure(log_name='Cluster', log_level=loglevel)

    private_cluster = False
    if nworkers or cluster:
        if not cluster:
            cluster = LocalCluster(n_workers=nworkers, threads_per_worker=1)
            logger.info(f"Initializing private cluster {cluster.scheduler_address} with {nworkers} workers.")
            private_cluster = True
        else:
            logger.info(f"Connecting to cluster {cluster}.")
        client = Client(cluster)
    else:
        client = None

    return client, cluster, private_cluster


def close_cluster(client, cluster, private_cluster, loglevel: str = 'WARNING'):
    """
    Close the dask cluster and client.

    Args:
        client (dask.distributed.Client): dask client
        cluster (dask.distributed.LocalCluster): dask cluster
        private_cluster (bool): whether the cluster is private
        loglevel (str): logging level
    """
    logger = log_configure(log_name='Cluster', log_level=loglevel)

    if client:
        client.close()
        logger.debug("Dask client closed.")

    if private_cluster:
        cluster.close()
        logger.debug("Dask cluster closed.")


def load_diagnostic_config(diagnostic: str, args: argparse.Namespace,
                           default_config: str = "config.yaml",
                           loglevel: str = 'WARNING'):
    """
    Load the diagnostic configuration file and return the configuration dictionary.

    Args:
        diagnostic (str): diagnostic name
        args (argparse.Namespace): arguments of the CLI. "config" argument can modify the default configuration file.
        default_config (str): default name configuration file (yaml format)
        loglevel (str): logging level. Default is 'WARNING'.

    Returns:
        dict: configuration dictionary
    """
    if args.config:
        filename = args.config
    else:
        configdir = ConfigPath(loglevel=loglevel).configdir
        filename = os.path.join(configdir, "diagnostics", diagnostic, default_config)

    return load_yaml(filename)


def merge_config_args(config: dict, args: argparse.Namespace):
    """
    Merge the configuration dictionary with the arguments of the CLI.

    Args:
        config (dict): configuration dictionary
        args (argparse.Namespace): arguments of the CLI

    Returns:
        dict: merged configuration dictionary
    """
    datasets = config['datasets']

    # Override the first dataset in the config file if provided in the command line
    datasets[0]['catalog'] = get_arg(args, 'catalog', datasets[0]['catalog'])
    datasets[0]['model'] = get_arg(args, 'model', datasets[0]['model'])
    datasets[0]['exp'] = get_arg(args, 'exp', datasets[0]['exp'])
    datasets[0]['source'] = get_arg(args, 'source', datasets[0]['source'])

    config['output']['outputdir'] = get_arg(args, 'outputdir', config['output']['outputdir'])

    return config

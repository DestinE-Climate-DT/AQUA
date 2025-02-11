# !/usr/bin/env python3
import argparse
import os
import sys
from dask.distributed import Client, LocalCluster

from aqua import __version__ as aquaversion
from aqua.logger import log_configure
from aqua.util import get_arg, load_yaml
from aqua.util import ConfigPath
from aqua.diagnostics.etccdi import SDII


def parse_arguments(cli_args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='ETCCDI SDII CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')
    parser.add_argument("--cluster", type=str,
                        required=False, help="dask cluster address")
    parser.add_argument('--catalog', type=str, help='catalog name',
                        required=False)
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--year', type=int, help='year',
                        required=False)

    return parser.parse_args(cli_args)

if __name__ == "__main__":
    
    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='ETCCDI-SDII', log_level=loglevel)

    logger.info(f'Running AQUA v{aquaversion} Teleconnections diagnostic')
   # Dask distributed cluster
    nworkers = get_arg(args, 'nworkers', None)
    cluster = get_arg(args, 'cluster', None)
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

    configdir = ConfigPath(loglevel=loglevel).configdir
    default_config = os.path.join(configdir, "diagnostics", "etccdi",
                                  "cli_config_etccdi.yaml")
    file = get_arg(args, 'config', default_config)
    logger.info('Reading configuration yaml file: {}'.format(file))
    config = load_yaml(file)

    models = config['models']
    catalog = get_arg(args, 'catalog', models[0]['catalog'])
    model = get_arg(args, 'model', models[0]['model'])
    exp = get_arg(args, 'exp', models[0]['exp'])
    source = get_arg(args, 'source', models[0]['source'])
    year = get_arg(args, 'year', config['year'])

    sdii_flag = config['indices'].get('SDII', False)

    if sdii_flag:
        sdii = SDII(model=model, exp=exp, source=source, catalog=catalog,
                    year=year, loglevel=loglevel)
        sdii.compute_index()
        sdii.combine_monthly_index()

    if client:
        client.close()
        logger.debug("Dask client closed.")

    if private_cluster:
        cluster.close()
        logger.debug("Dask cluster closed.")
    
    logger.info('ETCCDI SDII diagnostic finished.')
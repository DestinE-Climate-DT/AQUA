# !/usr/bin/env python3
import argparse
import os
import sys
from dask.distributed import Client, LocalCluster

from aqua import __version__ as aquaversion
from aqua.logger import log_configure
from aqua.util import get_arg, load_yaml
from aqua.util import ConfigPath
from aqua.diagnostics.core import template_parse_arguments, open_cluster, close_cluster
from aqua.diagnostics.etccdi import SDII


def parse_arguments(cli_args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='ETCCDI SDII CLI')

    parser = template_parse_arguments(parser)

    parser.add_argument('-n', '--nworkers', type=int,
                        help='number of dask distributed workers')
    parser.add_argument("--cluster", type=str,
                        required=False, help="dask cluster address")
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)
    parser.add_argument('--year', type=int, help='year',
                        required=False)
    parser.add_argument('--month', type=int, help='month to start the calculation from',
                        required=False)

    return parser.parse_args(cli_args)


if __name__ == "__main__":

    args = parse_arguments(sys.argv[1:])
    loglevel = get_arg(args, 'loglevel', 'WARNING')
    logger = log_configure(log_name='ETCCDI-SDII', log_level=loglevel)

    logger.info(f'Running AQUA v{aquaversion} Teleconnections diagnostic')

    nworkers = get_arg(args, 'nworkers', None)
    cluster = get_arg(args, 'cluster', None)
    client, cluster, private_cluster = open_cluster(nworkers, cluster, loglevel)

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
    month = get_arg(args, 'month', 1)

    sdii_flag = config['indices'].get('SDII', False)

    if sdii_flag:
        sdii = SDII(model=model, exp=exp, source=source, catalog=catalog,
                    year=year, loglevel=loglevel, month=month)
        sdii.compute_index()
        sdii.combine_monthly_index()

    close_cluster(client, cluster, private_cluster, loglevel)

    logger.info('ETCCDI SDII diagnostic finished.')

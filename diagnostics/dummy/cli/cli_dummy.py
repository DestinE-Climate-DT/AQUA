#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA dummy diagnositc command line interface.
Check that the imports are correct and the requested model is available in the
Reader catalogue.
'''
import os
import sys
import argparse


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Check setup CLI')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('--model_atm', type=str, help='model atm name',
                        required=False)
    parser.add_argument('--model_oce', type=str, help='model oce name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source', type=str, help='source name',
                        required=False)
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


if __name__ == '__main__':

    print('Running check setup CLI..')
    args = parse_arguments(sys.argv[1:])

    # change the current directory to the one of the CLI so that relative path works
    abspath = os.path.abspath(__file__)
    dname = os.path.dirname(abspath)
    if os.getcwd() != dname:
        os.chdir(dname)
        print(f'Moving from current directory to {dname} to run!')

    # Checking imports
    try:
        from aqua.__init__ import __version__ as aqua_version
        from aqua import Reader
        from aqua.logger import log_configure
        from aqua.util import get_arg, load_yaml
        from teleconnections.__init__ import __version__ as telec_version
    except ImportError:
        print('Failed to import aqua. Check that you have installed aqua.')
        print('If you have installed aqua, check that you have activated the conda environment.')
        sys.exit(1)
    except Exception as e:
        print('Failed to import aqua: {}'.format(e))
        sys.exit(1)
    else:
        loglevel = get_arg(args, 'loglevel', 'WARNING')
        logger = log_configure(log_name='Setup check', log_level='WARNING')
        logger.warning('Running aqua version {}'.format(aqua_version))

    file = get_arg(args, 'config', 'dummy_config.yaml')
    logger.info('Reading configuration from {}'.format(file))

    config = load_yaml(file)

    model_atm = get_arg(args, 'model_atm', config['dummy']['model'])
    model_oce = get_arg(args, 'model_oce', config['dummy']['model'])
    exp = get_arg(args, 'exp', config['dummy']['exp'])
    source = get_arg(args, 'source', config['dummy']['source'])

    try:
        reader_atm = Reader(model=model_atm, exp=exp, source=source,
                            loglevel=loglevel)
        reader_atm.retrieve()
    except Exception as e:
        logger.error('Failed to retrieve atm data: {}'.format(e))
        logger.warning('Check that the atm model is available in the Reader catalogue.')
        reader_atm = None

    try:
        reader_oce = Reader(model=model_oce, exp=exp, source=source,
                            loglevel=loglevel)
        reader_oce.retrieve()
    except Exception as e:
        logger.error('Failed to retrieve oce data: {}'.format(e))
        logger.warning('Check that the oce model is available in the Reader catalogue.')
        reader_oce = None

    if reader_atm is None and reader_oce is None:
        logger.error('Failed to retrieve any data. Check that the model name is correct.')
        sys.exit(1)
    else:
        if reader_atm is None:
            logger.error('Only oceanic data is available. Check that the atmospheric model name is correct.')
        if reader_oce is None:
            logger.error('Only atmospheric data is available. Check that the oceanic model name is correct.')

    logger.warning('Check complete, diagnostics can run!')

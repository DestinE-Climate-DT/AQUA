#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA dummy diagnositc command line interface. Reads configuration file and performs dummy 
diagnostic
'''
import sys
import argparse
from aqua.util import load_yaml, get_arg
sys.path.insert(0, '../')
from dummy_class_readerwrapper import DummyDiagnosticWrapper


def parse_arguments(args):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='Dummy diagnostic CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    return parser.parse_args(args)


if __name__ == '__main__':

    print('Running dummy diagnostic...')
    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', 'dummy_config.yaml')
    print('Reading configuration yaml file..')

    config = load_yaml(file)

    model = config['dummy']['model']
    exp = config['dummy']['exp']
    source = config['dummy']['source']
    var = config['dummy']['var']
    regrid = config['dummy']['regrid']
    loglevel= config['loglevel']

    loglevel = get_arg(args, 'loglevel', loglevel)

    dummy = DummyDiagnosticWrapper(model=model, exp=exp, source=source, var=var,
                                loglevel=loglevel, diagconfigdir='../config', regrid='r100')
    dummy.retrieve()
    dummy.multiplication()


    print('Dummy diagnostic test run completed. Go outside and live your life!')

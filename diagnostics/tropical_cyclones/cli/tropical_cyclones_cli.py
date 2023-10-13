#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""CLI interface to run the TempestExtemes TCs tracking"""

import argparse
import sys

#sys.path.insert(0, '../')

from tropical_cyclones import TCs
from aqua.util import load_yaml, get_arg
from aqua.logger import log_configure

def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Tropical Cyclones CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')

    parser.add_argument('-l', '--loglevel', type=str,
                        help='log level [default: WARNING]')

    # This arguments will override the configuration file if provided
    parser.add_argument('--model', type=str, help='model name',
                        required=False)
    parser.add_argument('--exp', type=str, help='experiment name',
                        required=False)
    parser.add_argument('--source2d', type=str, help='2d source name',
                        required=False)
    
    parser.add_argument('--source3d', type=str, help='3d source name',
                        required=False)

    return parser.parse_args(args)

if __name__ == '__main__':
    
    print('Running tropical cyclones diagnostic...')
    
    args = parse_arguments(sys.argv[1:])

    # Read configuration file

    file = get_arg(args, 'config', 'config_tcs.yaml')
    print('Reading tcs configuration yaml file.')
    config = load_yaml(file)

    # logger setup (via config or clommand line)

    loglevel = config['setup']['loglevel']
    loglevel = get_arg(args, 'loglevel', loglevel)
    logger = log_configure(log_level=loglevel, log_name='PI')

    
    # override config args in case they are passed from command line

    model = get_arg(args, 'model', config['dataset']['model'])
    exp = get_arg(args, 'exp', config['dataset']['exp'])
    source2d = get_arg(args, 'source2d', config['dataset']['source2d'])
    source3d = get_arg(args, 'source3d', config['dataset']['source3d'])

    # initialise tropical class with streaming options
    tropical = TCs(tdict=config, streaming=True, 
                    model=model,exp=exp, source2d=source2d, source3d=source3d,
                    stream_step=config['stream']['streamstep'], 
                    stream_unit="days", 
                    stream_startdate=config['time']['startdate'], 
                    loglevel = loglevel,
                    nproc=1)
    
    tropical.loop_streaming(config)
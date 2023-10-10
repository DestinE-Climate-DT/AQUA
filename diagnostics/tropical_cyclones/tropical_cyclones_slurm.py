#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""CLI interface to run the TempestExtemes TCs tracking"""

import argparse
import sys

sys.path.insert(0, '../')

from tropical_cyclones import TCs
from aqua.util import load_yaml, get_arg
from aqua.logger import log_configure

mainlogger = log_configure('INFO', log_name='MAIN')


def parse_arguments(args):
    """Parse command line arguments"""

    parser = argparse.ArgumentParser(description='Tropical Cyclones CLI')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-d', '--dry', action='store_true',
                        required=False,
                        help='if True, run is dry, no files are written')
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
    parser.add_argument('--outputdir', type=str, help='output directory',
                        required=False)

    return parser.parse_args(args)

if __name__ == '__main__':
    
    print('Running tropical cyclones diagnostic...')
    
    args = parse_arguments(sys.argv[1:])

    # Read configuration file

    file = get_arg(args, 'config', 'config/config_tcs.yaml')
    print('Reading tcs configuration yaml file.')
    config = load_yaml(file)
    
    #in case you passed args as commands

    loglevel = get_arg(args, 'loglevel', 'WARNING')

    model = get_arg(args, 'model', config['dataset']['model'])
    exp = get_arg(args, 'exp', config['dataset']['exp'])
    source2d = get_arg(args, 'source2d', config['dataset']['source2d'])
    source3d = get_arg(args, 'source3d', config['dataset']['source3d'])

    dry = get_arg(args, 'dry', False)
    
    if dry:
        print('Dry run, no files will be written')
        savefile = False
    else:
        savefile = True

    # initialise tropical class with streaming options
    tropical = TCs(tdict=config, streaming=True, 
                    model=model,exp=exp, source2d=source2d, source3d=source3d,
                    stream_step=config['stream']['streamstep'], 
                    stream_unit="days", 
                    stream_startdate=config['time']['startdate'], 
                    loglevel = loglevel,
                    nproc=1)
    
    tropical.loop_streaming(config)
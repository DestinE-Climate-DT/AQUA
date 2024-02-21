#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA wrapper for LRA CLI to submit parallel slurm job based on EACH variable
Use with caution, it can submit tens of sbatch jobs!
'''

import subprocess
import argparse
import yaml
import sys
from aqua.util import load_yaml, get_arg

def submit_sbatch(model, exp, source, varname, yaml_file, 
                  workers, definitive, overwrite):
    
    """
    Submit a sbatch script for the LRA CLI with basic options
    """

    # this is still optional, we can read from configuration file the default partition properties
    # for now it is still hardcoded with LUMI default
    with open(yaml_file, 'r') as stream:
        config = yaml.safe_load(stream)

    # create identifier for each model-exp-source-var tuple
    job_name = "_".join([model, exp, source, varname])

    # Construct basic sbatch command
    sbatch_cmd = [
        'sbatch',
        '--partition=' + config.get('partition', 'ju-strategic'),
        '--job-name=' + config.get('job_name', 'lra-generator_' + job_name),
        '--output=' + config.get('output', 'log/log-lra_' + job_name + '_%j.out'),
        '--error=' + config.get('error', 'log/log-lra_' + job_name + '_%j.err'),
        '--account=' + config.get('account', 'project_465000454'),
        '--nodes=' + str(config.get('nodes', 1)),
        '--ntasks-per-node=' + str(config.get('ntasks_per_node', workers)),
        '--time=' + config.get('time', '02:00:00')
        #'--mem=' + config.get('mem', '256G')
    ]

    # Add script command
    sbatch_cmd.append('./cli_lra_generator.py')
    sbatch_cmd.append('--config')
    sbatch_cmd.append(yaml_file)
    sbatch_cmd.append('--model')
    sbatch_cmd.append(model)
    sbatch_cmd.append('--exp')
    sbatch_cmd.append(exp)
    sbatch_cmd.append('--source')
    sbatch_cmd.append(source)
    sbatch_cmd.append('--var')
    sbatch_cmd.append(varname)
    sbatch_cmd.append('-w')
    sbatch_cmd.append(str(workers))

    # Execute sbatch command
    if definitive:
        if overwrite:
            sbatch_cmd.append('-o')    
        sbatch_cmd.append('-d')
        subprocess.run(sbatch_cmd)
    else:
        print(sbatch_cmd)

def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA LRA parallel SLURM generator')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file', required=True)
    parser.add_argument('-d', '--definitive', action="store_true",
                        help='definitive run with files creation')
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing LRA files')
    parser.add_argument('-w', '--workers', type=str,
                        help='number of dask workers. Default is 8')

    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    file = get_arg(args, 'config', None)
    workers = get_arg(args, 'workers', 8)
    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    print('Reading configuration yaml file..')

    # loading the usual configuration file
    config = load_yaml(file)

    # sbatch looping
    for model in config['catalog'].keys():
        for exp in config['catalog'][model].keys():
            for source in config['catalog'][model][exp].keys():
                varnames = config['catalog'][model][exp][source]['vars']
                for varname in varnames:
                    print(' '.join(['Submitting', model, exp, source, varname]))
                    submit_sbatch(model, exp, source, varname, file, 
                                  workers, definitive, overwrite)

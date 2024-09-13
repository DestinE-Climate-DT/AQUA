#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA wrapper for LRA CLI to submit parallel slurm job based on EACH variable
Use with caution, it can submit tens of sbatch jobs!
'''

import subprocess
import argparse
import re
import sys
from aqua.util import load_yaml, get_arg

def is_job_running(job_name, username):
    """verify that a job name is not already submitted in the slurm queue"""
    # Run the squeue command to get the list of jobs
    output = subprocess.run(['squeue', '-u', username, '--format', '%j'], 
                            capture_output=True, check=True)
    output = output.stdout.decode('utf-8').splitlines()[1:]
    
    # Parse the output to check if the job name is in the list
    return job_name in output

def submit_sbatch(model, exp, source, varname, slurm_dict, yaml_file,
                  workers=1, definitive=False, overwrite=False, dependency=None):
    
    """
    Submit a sbatch script for the LRA CLI with basic options

    args:
        model: model to be processed
        exp: exp to be processed
        source: source to be processed
        varname: varname to be processed
        slurm_dict: dictionary with slurm optiosn
        yaml_file: config file for submission
        workers: dask workers
        definitive: produce the LRA
        overwrite: overwrite the LRA
        dependency: jobid on which dependency of slurm is built

    Return
        jobid
    """

    # create identifier for each model-exp-source-var tuple
    job_name = "_".join([model, exp, source, varname])
    full_job_name = 'lra-generator_' + job_name

    # username
    username = slurm_dict.get('username', 'padavini')

    # Construct basic sbatch command
    sbatch_cmd = [
        'sbatch',
        '--partition=' + slurm_dict.get('partition', 'small'),
        '--job-name=' + slurm_dict.get('job_name', full_job_name),
        '--output=log/overnight-lra_' + job_name + '_%j.out',
        '--error=log/overnight-lra_' + job_name + '_%j.err',
        '--account=' + slurm_dict.get('account', 'project_465000454'),
        '--nodes=' + str(slurm_dict.get('nodes', 1)),
        '--ntasks-per-node=' + str(slurm_dict.get('ntasks_per_node', workers)),
        '--time=' + slurm_dict.get('time', '08:00:00'),
        '--mem=' + slurm_dict.get('mem', '256G')
        #'--priority=low',
    ]

    if dependency is not None:
        sbatch_cmd.append('--dependency=afterany:'+ str(dependency))

    # Add script command
    sbatch_cmd.append('aqua lra')
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

    if is_job_running(full_job_name, username):
        print(f'The job is {job_name} is already running, will not resubmit')
        return 0

    # Execute sbatch command
    if definitive:
        if overwrite:
            sbatch_cmd.append('-o')
        sbatch_cmd.append('-d')
        result = subprocess.run(sbatch_cmd, capture_output = True, check=True).stdout.decode('utf-8')
        jobid = re.findall(r'\b\d+\b', result)[-1]
        return jobid

    #print(sbatch_cmd)
    return 0


def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA LRA parallel SLURM generator')
    parser.add_argument('-c', '--config', type=str,
                        help='AQUA yaml configuration file', required=True)
    parser.add_argument('-s', '--slurm', type=str,
                        help='SLURM yaml configuration file')
    parser.add_argument('-d', '--definitive', action="store_true",
                        help='definitive run with files creation')
    parser.add_argument('-o', '--overwrite', action="store_true",
                        help='overwrite existing LRA files')
    parser.add_argument('-w', '--workers', type=str,
                        help='number of dask workers. Default is 8')
    parser.add_argument('-p', '--parallel', type=str,
                        help='number of parallel jobs to be runs. Default is 5')

    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])
    config_file = get_arg(args, 'config', None)
    slurm_file = get_arg(args, 'slurm', None)
    workers = get_arg(args, 'workers', 8)
    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    parallel = get_arg(args, 'parallel', 5)
    print('Reading configuration yaml file..')

    # loading the usual configuration file
    config = load_yaml(config_file)
    slurm = config.get('slurm', {})

    # sbatch looping
    COUNT = 0 # to count job
    jobid = None
    PARENT_JOB = None # to define the parent job for dependency
    for model in config['data'].keys():
        for exp in config['data'][model].keys():
            for source in config['data'][model][exp].keys():
                varnames = config['data'][model][exp][source]['vars']
                for varname in varnames:
                    if (COUNT % int(parallel)) == 0 and COUNT != 0:
                        print('Updating parent job to' + jobid)
                        PARENT_JOB = str(jobid)
                    COUNT = COUNT + 1
                    print(' '.join(['Submitting', model, exp, source, varname]))
                    jobid = submit_sbatch(model=model, exp=exp, source=source, varname=varname,
                                          slurm_dict=slurm, yaml_file=config_file,
                                          workers=workers, definitive=definitive,
                                          overwrite=overwrite, dependency=PARENT_JOB)

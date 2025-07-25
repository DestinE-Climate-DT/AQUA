#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA wrapper for LRA CLI to submit parallel slurm job based on EACH variable
Use with caution, it can submit tens of sbatch jobs!
'''

import subprocess
import argparse
import re
import os
import sys
import jinja2
from aqua.util import load_yaml, get_arg, ConfigPath, to_list

def is_job_running(job_name, username):
    """verify that a job name is not already submitted in the slurm queue"""
    # Run the squeue command to get the list of jobs
    output = subprocess.run(['squeue', '-u', username, '--format', '%j'], 
                            capture_output=True, check=True)
    output = output.stdout.decode('utf-8').splitlines()[1:]
    
    # Parse the output to check if the job name is in the list
    return job_name in output

def load_jinja_template(template_file='aqua_lra.j2'):
    """
    Load a Jinja2 template.

    Args:
        template_file (str): Template file name.

    Returns:
        jinja2.Template: Loaded Jinja2 template.
    """

    templateloader = jinja2.FileSystemLoader(searchpath=os.path.dirname(template_file))
    templateenv = jinja2.Environment(loader=templateloader, trim_blocks=True, lstrip_blocks=True)
    if os.path.exists(template_file):
        return templateenv.get_template(os.path.basename(template_file))
    
    raise FileNotFoundError(f'Cannot file template file {template_file}')

def submit_sbatch(model, exp, source, varname, realization, slurm_dict, yaml_file,
                  workers=1, definitive=False, overwrite=False,
                  dependency=None, singularity=None):
    
    """
    Submit a sbatch script for the LRA CLI with basic options

    args:
        model: model to be processed
        exp: exp to be processed
        source: source to be processed
        varname: varname to be processed
        realization: realization to be processed
        slurm_dict: dictionary with slurm optiosn
        yaml_file: config file for submission
        workers: dask workers
        definitive: produce the LRA
        overwrite: overwrite the LRA
        dependency: jobid on which dependency of slurm is built
        singularity: Run with the available AQUA container

    Return
        jobid
    """

    # create identifier for each model-exp-source-var tuple
    job_name = "_".join([model, exp, source, varname])
    full_job_name = 'lra-generator_' + job_name
    log_dir = 'log'

    # bit complicated way to get the AQUA main path
    aquapath =  os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

    jinjadict = {
        'job_name': full_job_name,
        'username': slurm_dict.get('username', 'padavini'),
        'partition': slurm_dict.get('partition', 'debug'),
        'log_output': f'{log_dir}/overnight-lra_{job_name}_%j.out',
        'log_error': f'{log_dir}/overnight-lra_{job_name}_%j.err',
        'account': slurm_dict.get('account', 'project_465000454'),
        'nodes': str(slurm_dict.get('nodes', 1)),
        'ntasks_per_node': str(slurm_dict.get('ntasks_per_node', workers)),
        'time': slurm_dict.get('time', '00:29:00'),
        'memory': slurm_dict.get('mem', '128G'),
        'singularity': singularity,
        'dependency': dependency,
        'model': model,
        'exp': exp,
        'source': source,
        'varname': varname,
        'realization': realization,
        'definitive': definitive,
        'overwrite': overwrite,
        'config': yaml_file,
        'machine': ConfigPath().get_machine(),
        'aqua': aquapath,
    }

    if is_job_running(full_job_name, jinjadict['username']):
        print(f'The job is {job_name} is already running, will not resubmit')
        return 0

    template = load_jinja_template()
    render = template.render(jinjadict)
    #print(render)

    tempfile = 'tempfile.job'
    with open(tempfile, "w", encoding='utf8') as fh:
        fh.write(render)

    sbatch_cmd = ['sbatch', tempfile]

    # Execute sbatch command
    if definitive:
        try:
            #command_str = ' '.join(sbatch_cmd)
            #result = subprocess.run(command_str, shell=True, capture_output = True, check=True, env=os.environ).stdout.decode('utf-8')
            result = subprocess.run(sbatch_cmd, capture_output = True, check=True).stdout.decode('utf-8')
            jobid = re.findall(r'\b\d+\b', result)[-1]
            if os.path.exists(tempfile):
                os.remove(tempfile)
            return jobid
        except subprocess.CalledProcessError as e:
            # Print the error message and stderr if the command fails
            print(f"Command failed with return code {e.returncode}")
            print(f"stdout: {e.stdout}")
            print(f"stderr: {e.stderr}")

    print(sbatch_cmd)
    return 0


def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA LRA parallel SLURM generator')
    parser.add_argument('-c', '--config', type=str,
                        help='AQUA yaml configuration file', required=True)
    parser.add_argument('-s', '--singularity', action="store_true",
                        help='run with singualirity container')
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
    workers = get_arg(args, 'workers', 8)
    definitive = get_arg(args, 'definitive', False)
    overwrite = get_arg(args, 'overwrite', False)
    singularity = get_arg(args, 'singularity', None)
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
                if 'realizations' in config['data'][model][exp][source].keys():
                    realizations = to_list(config['data'][model][exp][source]['realizations'])
                else:
                    realizations = [1]
                for realization in realizations:
                    varnames = config['data'][model][exp][source]['vars']
                    for varname in varnames:
                        if (COUNT % int(parallel)) == 0 and COUNT != 0:
                            print('Updating parent job to' + str(jobid))
                            PARENT_JOB = str(jobid)
                        COUNT = COUNT + 1
                        print(' '.join(['Submitting', model, exp, source, varname]))
                        jobid = submit_sbatch(model=model, exp=exp, source=source,
                                            varname=varname, realization=realization,
                                            slurm_dict=slurm, yaml_file=config_file,
                                            workers=workers, definitive=definitive,
                                            overwrite=overwrite, dependency=PARENT_JOB,
                                            singularity=singularity)

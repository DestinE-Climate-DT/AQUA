#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA cli tool to submit parallel aqua-web slurm jobs
'''

import subprocess
import argparse
import re
import sys
import os
from jinja2 import Template
from ruamel.yaml import YAML
from tempfile import NamedTemporaryFile
from aqua.logger import log_configure
from aqua.util import get_arg, ConfigPath


class Submitter():
    """
    Class to submit AQUA jobs to the slurm queue with logging
    """

    def __init__(self, loglevel='INFO', config='config.aqua-web.yaml',
                 template='aqua-web.job.j2', dryrun=False, parallel=True):
        """
        Initialize the Submitter class

        args:
            loglevel: logging level
            config: yaml configuration file base name
            template: jinja template file base name
            dryrun: perform a dry run (no job submission)
            parallel: run in parallel mode (multiple cores)
        """

        if dryrun:
            loglevel="debug"

        self.logger = log_configure(log_level=loglevel, log_name='aqua-web')

        self.config, self.template = self.find_config_files(config, template)

        self.dryrun = dryrun
        self.parallel = parallel

    def is_job_running(self, job_name, username):
        """verify that a job name is not already submitted in the slurm queue"""
        # Run the squeue command to get the list of jobs
        output = subprocess.run(['squeue', '-u', username, '--format', '%j'], 
                                capture_output=True, check=True)
        output = output.stdout.decode('utf-8').splitlines()[1:]
        
        # Parse the output to check if the job name is in the list
        return job_name in output

    def submit_sbatch(self, model, exp, source=None, dependency=None):
        """
        Submit a sbatch script for the LRA CLI with basic options

        args:
            model: model to be processed
            exp: exp to be processed
            source: source to be processed
            dependency: jobid on which dependency of slurm is built

        Return
            jobid
        """

        yaml = YAML(typ='rt')
        with open(self.config, 'r', encoding='utf-8') as file:
            definitions = yaml.load(file)

        if model:
            definitions['model'] = model
        else:
            model = definitions['model']
        if exp:
            definitions['exp'] = exp
        else:
            exp = definitions['exp']
        if source:
            definitions['source'] = source
        else:
            source = definitions['source']
        if self.parallel:
            definitions['parallel'] = '-p'
        else:
            definitions['parallel'] = ''

        username = definitions['username']
        jobname = definitions.get('jobname', 'aqua-web')

        # create identifier for each model-exp-source-var tuple
        full_job_name = jobname + '_' + "_".join([model, exp, source])
        definitions['job_name'] = full_job_name

        definitions['output'] = full_job_name + '_%j.out'
        definitions['error'] = full_job_name + '_%j.err'

        with open(self.template, 'r', encoding='utf-8') as file:
            rendered_job  = Template(file.read()).render(definitions)

        with NamedTemporaryFile('w', delete=False) as tempfile:
            tempfile.write(rendered_job)
            job_temp_path = tempfile.name

        if self.dryrun:
            self.logger.debug("SLURM job:\n %s", rendered_job)
        
        sbatch_cmd = [ 'sbatch' ]

        if dependency is not None:
            sbatch_cmd.append('--dependency=afterany:'+ str(dependency))

        sbatch_cmd.append(job_temp_path)

        if not self.dryrun and self.is_job_running(full_job_name, username):
            self.logger.info('The job is %s is already running, will not resubmit', full_job_name)
            return 0

        if not self.dryrun:
            self.logger.info('Submitting %s %s %s', model, exp, source)
            result = subprocess.run(sbatch_cmd, capture_output = True, check=True).stdout.decode('utf-8')
            jobid = re.findall(r'\b\d+\b', result)[-1]
            return jobid
        else:
            self.logger.debug('SLURM job name: %s', full_job_name)
            return 0


    def find_config_files(self, config, template):
        """
        Find the configuration and template files
        """

        # Find the AQUA config directory
        configurer = ConfigPath()

        script_location = os.path.dirname(os.path.abspath(__file__))
        search_paths = [configurer.configdir, '.', script_location]

        # If the config file does not exists find it either in the AQUA config dir, the location of the script or here
        if not os.path.isfile(config):
            found_config = False
            for path in search_paths:
                config_path = os.path.join(path, config)
                if os.path.exists(config_path):
                    config = config_path
                    found_config = True
                    break
            if not found_config:
                raise FileNotFoundError(f"Config file '{config}' not found in search paths: {search_paths}")

        if not os.path.isfile(template):
            found_config = False
            for path in search_paths:
                template_path = os.path.join(path, template)
                if os.path.exists(template_path):
                    template = template_path
                    found_config = True
                    break
            if not found_config:
                raise FileNotFoundError(f"Template file '{config}' not found in search paths: {search_paths}")

        self.logger.debug("Using configuration file: %s", config)
        self.logger.debug("Using job template: %s", template)
        return config, template


def parse_arguments(arguments):
    """
    Parse command line arguments
    """

    parser = argparse.ArgumentParser(description='AQUA aqua-web CLI tool to submit parallel diagnostic jobs')

    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-m', '--model', type=str,
                        help='model to be processed')
    parser.add_argument('-e', '--exp', type=str,
                        help='experiment to be processed')
    parser.add_argument('-s', '--source', type=str,
                        help='source to be processed')
    parser.add_argument('-r', '--serial', action="store_true",
                        help='run in serial mode (only one core)')
    parser.add_argument('-x', '--max', type=int,
                        help='max number of jobs to submit without dependency')
    parser.add_argument('-t', '--template', type=str,
                        help='template jinja file for slurm job')
    parser.add_argument('-d', '--dry', action="store_true",
                        help='perform a dry run (no job submission)')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='logging level')
    
    # List of experiments is a positional argument
    parser.add_argument('list', nargs='?', type=str,
                        help='list of experiments in format: model, exp, source')

    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    model = get_arg(args, 'model', None)
    exp = get_arg(args, 'exp', None)
    source = get_arg(args, 'source', None)
    config = get_arg(args, 'config', 'config.aqua-web.yaml')
    serial = get_arg(args, 'serial', False)
    listfile = get_arg(args, 'list', None)
    dependency = get_arg(args, 'max', None)
    template = get_arg(args, 'template', 'aqua-web.job.j2')
    dryrun = get_arg(args, 'dry', False)
    loglevel = get_arg(args, 'loglevel', 'info')

    submitter = Submitter(config=config, template=template, dryrun=dryrun, parallel=not serial, loglevel=loglevel)

    count = 0
    parent_job = None
    jobid = None

    if listfile:
        with open(listfile, 'r') as file:
            for line in file:

                line = line.strip()
                if not line or line.startswith('#'):
                    continue
    
                model, exp, *source = re.split(r',|\s+|\t+', line.strip())  # split by comma, space, tab

                if len(source) == 0:
                    source = None

                if dependency and (count % dependency == 0) and count != 0:
                            submitter.logger.info('Updating parent job to %s', str(jobid))
                            parent_job = str(jobid)

                count = count + 1
                
                jobid = submitter.submit_sbatch(model, exp, source=source, dependency=parent_job)           
    else:
        jobid = submitter.submit_sbatch(model, exp, source=source, dependency=parent_job)

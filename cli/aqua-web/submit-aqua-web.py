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
import uuid
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
                 template='aqua-web.job.j2', dryrun=False, parallel=True,
                 wipe=False, native=False, fresh=False):
        """
        Initialize the Submitter class

        args:
            loglevel: logging level
            config: yaml configuration file base name
            template: jinja template file base name
            dryrun: perform a dry run (no job submission)
            parallel: run in parallel mode (multiple cores)
            wipe: wipe the destination directory before copying the images
            native: use the native AQUA version (default is the container version)
            fresh: use a fresh (new) output directory, do not recycle original one
        """

        if dryrun:
            loglevel="debug"

        self.logger = log_configure(log_level=loglevel, log_name='aqua-web')

        self.config, self.template = self.find_config_files(config, template)

        self.dryrun = dryrun
        self.parallel = parallel
        self.wipe = wipe
        self.native = native
        if fresh:
            self.fresh = f"/tmp{str(uuid.uuid4())[:13]}"
        else:
            self.fresh = ""

    def is_job_running(self, job_name, username):
        """verify that a job name is not already submitted in the slurm queue"""
        # Run the squeue command to get the list of jobs
        output = subprocess.run(['squeue', '-u', username, '--format', '%j'], 
                                capture_output=True, check=True)
        output = output.stdout.decode('utf-8').splitlines()[1:]
        
        # Parse the output to check if the job name is in the list
        return job_name in output

    def submit_sbatch(self, catalog, model, exp, source=None, dependency=None):
        """
        Submit a sbatch script with basic options

        args:
            catalog: catalog for experiment
            model: model to be processed
            exp: experiment to be processed
            source: source to be processed
            dependency: jobid on which dependency of slurm is built

        Return
            jobid
        """

        yaml = YAML(typ='rt')
        with open(self.config, 'r', encoding='utf-8') as file:
            definitions = yaml.load(file)

        if catalog:
            definitions['catalog'] = catalog
        else:
            catalog = definitions['catalog']
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
        if self.wipe:
            definitions['wipe'] = '-w'
        else:
            definitions['wipe'] = ''
        if self.native:
            definitions['nativeaqua'] = 'true'
        else:
            definitions['nativeaqua'] = 'false'

        definitions['fresh'] = self.fresh

        username = definitions['username']
        jobname = definitions.get('jobname', 'aqua-web')

        # create identifier for each model-exp-source-var tuple
        full_job_name = jobname + '_' + "_".join([catalog, model, exp, source])
        definitions['job_name'] = full_job_name

        definitions['output'] = full_job_name + '_%j.out'
        definitions['error'] = full_job_name + '_%j.err'

        definitions['push'] = "false"

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
        self.logger.debug("sbatch command: %s", sbatch_cmd)

        if not self.dryrun and self.is_job_running(full_job_name, username):
            self.logger.info('The job %s is already running, will not resubmit', full_job_name)
            return '0'

        if not self.dryrun:
            self.logger.info('Submitting %s %s %s', model, exp, source)
            result = subprocess.run(sbatch_cmd, capture_output = True, check=True).stdout.decode('utf-8')
            jobid = re.findall(r'\b\d+\b', result)[-1]
            return jobid
        else:
            self.logger.debug('SLURM job name: %s', full_job_name)
            return '0'


    def submit_push(self, jobid_list, listfile):
        """
        Submit a sbatch script for pushing to aqua-web

        Args:
            jobid_list: list of jobids for dependency
            listfile: file with list of experiments or "$model$/$exp" string
        """

        self.logger.info('Submitting push job %s', listfile)

        yaml = YAML(typ='rt')
        with open(self.config, 'r', encoding='utf-8') as file:
            definitions = yaml.load(file)

        username = definitions['username']
        full_job_name = definitions.get('jobname', 'push-aqua-web')

        definitions['job_name'] = full_job_name
        definitions['output'] = full_job_name + '_%j.out'
        definitions['error'] = full_job_name + '_%j.err'

        definitions['push'] = "true"
        definitions['explist'] = listfile
        if self.wipe:
            definitions['wipe'] = '-w'
        else:
            definitions['wipe'] = ''
        if self.native:
            definitions['nativeaqua'] = 'true'
        else:
            definitions['nativeaqua'] = 'false'

        definitions['fresh'] = self.fresh

        with open(self.template, 'r', encoding='utf-8') as file:
            rendered_job  = Template(file.read()).render(definitions)

        with NamedTemporaryFile('w', delete=False) as tempfile:
            tempfile.write(rendered_job)
            job_temp_path = tempfile.name

        if self.dryrun:
            self.logger.debug("SLURM job:\n %s", rendered_job)
        
        sbatch_cmd = [ 'sbatch' ]

        sbatch_cmd.append('--dependency=afterany:' + ":".join(jobid_list))

        sbatch_cmd.append(job_temp_path)
        self.logger.debug("sbatch command: %s", sbatch_cmd)

        if not self.dryrun and self.is_job_running(full_job_name, username):
            self.logger.info('The job is %s is already running, will not resubmit', full_job_name)
            return '0'

        if not self.dryrun:
            self.logger.info('Submitting push job')
            result = subprocess.run(sbatch_cmd, capture_output = True, check=True).stdout.decode('utf-8')
            jobid = re.findall(r'\b\d+\b', result)[-1]
            return jobid
        else:
            self.logger.debug('SLURM job name: %s', full_job_name)
            return '0'


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
    parser.add_argument('-k', '--catalog', type=str,
                        help='catalog for experiment')
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
    parser.add_argument('-p', '--push', action="store_true",
                        help='flag to push to aqua-web')
    parser.add_argument('-w', '--wipe', action="store_true",
                        help='wipe the destination directory before copying the images')
    parser.add_argument('-n', '--native', action="store_true",
                        help='use the native (native) AQUA version (default is the container version)')
    parser.add_argument('-f', '--fresh', action="store_true",
                        help='use a fresh (new) output directory, do not recycle original one')
     
    # List of experiments is a positional argument
    parser.add_argument('list', nargs='?', type=str,
                        help='list of experiments in format: model, exp, source')

    return parser.parse_args(arguments)


if __name__ == '__main__':

    args = parse_arguments(sys.argv[1:])

    catalog = get_arg(args, 'catalog', None)
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
    push = get_arg(args, 'push', False)
    wipe = get_arg(args, 'wipe', False)
    native = get_arg(args, 'native', False)
    fresh = get_arg(args, 'fresh', False)

    submitter = Submitter(config=config, template=template, dryrun=dryrun,
                          parallel=not serial, wipe=wipe, native=native,
                          fresh=fresh, loglevel=loglevel)

    count = 0
    parent_job = None
    jobid = None

    jobid_list = []

    if listfile:
        with open(listfile, 'r') as file:
            for line in file:

                line = line.strip()
                if not line or line.startswith('#'):
                    continue
    
                catalog, model, exp, *source = re.split(r',|\s+|\t+', line.strip())  # split by comma, space, tab

                if len(source) == 0:
                    source = None

                if dependency and (count % dependency == 0) and count != 0:
                            submitter.logger.info('Updating parent job to %s', str(jobid))
                            parent_job = str(jobid)

                count = count + 1
                
                jobid = submitter.submit_sbatch(catalog, model, exp, source=source, dependency=parent_job)
                jobid_list.append(jobid)
    
        if push:
            submitter.submit_push(jobid_list, listfile)     

    else:
        jobid = submitter.submit_sbatch(catalog, model, exp, source=source, dependency=parent_job)
        if push:
            submitter.submit_push([jobid], f'{model}/{exp}') 

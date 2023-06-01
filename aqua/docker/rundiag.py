"""Runs an AQUA diagnostic using Docker"""

import os
import docker
from jinja2 import Template, FileSystemLoader, Environment
from aqua.util import load_yaml
from ruamel.yaml import YAML

def rundiag(cmd = None, config = '.',
            machine_file=r'machine.yaml',
            diagnostic_file=r'diagnostic.yaml'):
    """
    Run a diagnostics in docker.

    Args:
        cmd (:obj:`str`, optional):             The command to run, defined in machine.yaml.
                                                Defaults to None.
        machine_file (:obj:`str`, optional):    Name of machine defintion yaml file.
                                                Could also be directly a :obj:`dict`.
                                                Defaults to 'machine.yaml'.
        diagnostic_file (:obj:`str`, optional): Name of diagnostic defintion yaml file.
                                                Could also be directly a :obj:`dict`.
                                                Defaults to 'diagnostic.yaml'.
        config (:obj:`str`, optional):          Path of directory containing config files and templates.
                                                Defaults to '.'.

    Returns:
        The command text output.
    """

    if machine_file is dict:
        # We can also pass directly a dict because the config file has been read before
        machinecfg = machine_file
    else:
        # Import directory setting and volumes to mount from machine.yaml
        machinecfg = load_yaml(machine_file)

    # Transform volumes dict into "host=bind" strings
    volume_list = []
    for key, value in machinecfg['volumes'].items():
        volume_list += [f'{value}:/{key}']

    if diagnostic_file is dict:
        diagcfg = diagnostic_file
    else:
        # Import parameters needed to render templates
        diagcfg = load_yaml(diagnostic_file)

    templateLoader = FileSystemLoader(searchpath=f"{config}/templates")
    templateEnv = Environment(loader=templateLoader)

    # Now use jinja2 to render al top-level keys in recipe file
    # as templates into actual recipes, config files etc.

    for k in diagcfg:
        dest_file = os.path.join(machinecfg['volumes'][diagcfg[k]['target_volume']], diagcfg[k]['filename'])
        template_file = diagcfg[k]['template']
        template = templateEnv.get_template(template_file)
        template.stream(diagcfg[k]['parameters']).dump(dest_file)

    # Run actual diagnostic docker
    if not cmd:
        cmd_value = list(machinecfg['command'].values())[0]
    else:
        cmd_value = machinecfg['command'][cmd]

    client = docker.from_env()
    output = client.containers.run(image=machinecfg['image'],
                        command=cmd_value, 
                        volumes=volume_list, 
                        environment=machinecfg['environment'])
    return output

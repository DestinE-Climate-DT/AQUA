#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
AQUA basic tool for generating catalog entries based on jinja
'''

import jinja2
import os
import sys
import argparse
from aqua.util import ConfigPath, load_yaml, dump_yaml, get_arg
from aqua.logger import log_configure

def parse_arguments(arguments):
    """
    Parse command line arguments for the LRA CLI
    """

    parser = argparse.ArgumentParser(description='AQUA FDB entries generator')
    parser.add_argument('-c', '--config', type=str,
                        help='yaml configuration file')
    parser.add_argument('-j', '--jinja', type=str,
                        help='jinja template file')
    parser.add_argument('-l', '--loglevel', type=str,
                        help='loglevel', default='INFO')

    return parser.parse_args(arguments)

if __name__ == '__main__':

    # parsing
    args = parse_arguments(sys.argv[1:])
    definitions_file = get_arg(args, 'config', 'config.tmpl')
    jinja_file = get_arg(args, 'jinja', False)
    loglevel = get_arg(args, 'loglevel', 'WARNING')

    if not jinja_file:
        raise FileNotFoundError('You need to specify a jinja file for templating')

    # setup logger
    logger = log_configure(loglevel, 'FDB-catalog-generator')

    # reading files
    definitions = load_yaml(definitions_file)
    default = load_yaml('default.yaml')
    #levels for IFS-NEMO:
    definitions = {**definitions, **default[definitions['model']]}

    #eccodes path
    definitions['eccodes_path'] = '/projappl/project_465000454/jvonhar/aqua/eccodes/eccodes-' + definitions['eccodes_version'] + '/definitions'

    # jinja2 loading and replacing (to be checked)
    templateLoader = jinja2.FileSystemLoader(searchpath='./')
    templateEnv = jinja2.Environment(loader=templateLoader, trim_blocks=True, lstrip_blocks=True)

    template = templateEnv.get_template(jinja_file)
    outputText = template.render(definitions)


    #create output file in model folder
    configurer = ConfigPath()
    catalog_path, fixer_folder, config_file = configurer.get_reader_filenames()

    output_dir = os.path.join(os.path.dirname(catalog_path), 'catalog', definitions['model'])
    output_filename = f"{definitions['exp']}.yaml"
    output_path = os.path.join(output_dir, output_filename)

    with open(output_path, "w", encoding='utf8') as output_file:
        output_file.write(outputText)

    logger.info("File %s has been created in %s", output_filename, output_dir)

    #update main.yaml
    main_yaml_path = os.path.join(output_dir, 'main.yaml')

    main_yaml = load_yaml(main_yaml_path)
    main_yaml['sources'][definitions['exp']] = {
        'description': definitions['description'],
        'driver': 'yaml_file_cat',
        'args': {
            'path': f"{{{{CATALOG_DIR}}}}/{definitions['exp']}.yaml"
        }
    }

    dump_yaml(main_yaml_path, main_yaml)

    logger.info("%s entry in 'main.yaml' has been updated in %s", definitions['exp'], output_dir)
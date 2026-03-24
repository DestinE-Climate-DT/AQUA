#!/usr/bin/env python3
# -*- coding: utf-8 -*-

'''
AQUA basic tool for generating catalog entries based on jinja
'''

import os
import re
import sys
import argparse
import jinja2

from aqua.core.util import load_yaml, dump_yaml, get_arg
from aqua.core.configurer import ConfigPath
from aqua.core.logger import log_configure

from aqua.core.lock import SafeFileLock
from ruamel.yaml import YAML
yaml = YAML()
yaml.default_flow_style = None  # Ensure default flow style is None


def catgen_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description='AQUA FDB entries generator')

    parser.add_argument("-p", "--portfolio", help="Type of Data Portfolio utilized (full/reduced/minimal)")
    parser.add_argument('-c', '--config', type=str, help='yaml configuration file', required=True)
    parser.add_argument('-l', '--loglevel', type=str, help='loglevel', default='INFO')

    return parser

def get_nested(cfg, key):
    if isinstance(key, tuple):
        value = cfg
        for k in key:
            value = value.get(k) if value else None
        return value
    return cfg.get(key)

class AquaFDBGenerator:
    def __init__(self, data_portfolio, config_path, loglevel='INFO'):

        # config reading
        self.config = load_yaml(config_path)
        self.dp_version = data_portfolio

        # logging
        self.loglevel = loglevel
        self.logger = log_configure(self.loglevel, 'FDB catalog generator')

        # get the templates and config files from the AQUA installation
        self.catgendir = os.path.join(ConfigPath().configdir, 'catgen')
        self.logger.debug("Reading configuration files from %s", self.catgendir)
        self.template = self.load_jinja_template(os.path.join(self.catgendir, "catalog_entry.j2"))
        self.matching_grids = load_yaml(os.path.join(self.catgendir, "matching_grids.yaml"))


        # config options
        required_keys = [
            "author",
            "machine",
            ("repos", "data-portfolio_path"),
            ("repos", "Climate-DT-catalog_path"),
            "model",
            "resolution",
            "activity",
            "experiment",
            "expver",
            "expid",
            "data_start_date"
        ]

        # check missing parameters in config file
        missing = [k if isinstance(k, str) else ".".join(k)
                for k in required_keys if not get_nested(self.config, k)]

        if missing:
            raise ValueError(f"Missing required configuration keys: {', '.join(missing)}")

        # config options
        self.author = self.config['author']
        self.machine = self.config['machine']
        self.dp_dir_path = self.config["repos"]["data-portfolio_path"]
        self.catalog_dir_path = self.config["repos"]["Climate-DT-catalog_path"]
        self.model = self.config["model"].lower()
        #self.portfolio = self.config["portfolio"]
        self.resolution = self.config["resolution"]
        self.ocean_grid = self.config.get("ocean_grid")
        self.atm_grid = self.config.get("atm_grid")
        self.num_of_realizations = int(self.config.get("num_of_realizations", 1))
        self.description = None
        self.grid_resolutions = None


        # portfolio
        self.logger.info("Running FDB catalog generator for %s portfolio for model %s", data_portfolio, self.model)
        self.dp = load_yaml(os.path.join(self.dp_dir_path, 'portfolios', data_portfolio, 'portfolio.yaml'))
        self.grids = load_yaml(os.path.join(self.dp_dir_path, 'portfolios', data_portfolio, 'grids.yaml'))
        self.levels = load_yaml(os.path.join(self.dp_dir_path, 'definitions', 'levels.yaml'))

        self.local_grids = self.get_local_grids(self.resolution, self.grids)


    def get_local_grids(self, resolution, grids):
        """
        Get local grids based on the portfolio.

        Args:
            resolution (str): The portfolio resolution type.
            grids (dict): The grids definition.

        Returns:
            dict: Local grids for the given portfolio.
        """
        local_grids = grids["common"]
        if resolution in grids:
            self.logger.debug('Update grids for specific %s portfolio', resolution)
            local_grids.update(grids[resolution])
        else:
            self.logger.error('Cannot find grids for %s portfolio', resolution)
        print(local_grids)
        return local_grids

    def get_available_resolutions(self, local_grids, model):
        """
        Get available resolutions from local grids.

        Args:
            local_grids (dict): Local grids definition.
            model (str): The model name.

        Returns:
            list: List of available grid resolutions.
        """
        re_pattern = f"horizontal-{model.upper()}-(.+)"
        grid_resolutions = [match.group(1) for key in local_grids if (match := re.match(re_pattern, key))]
        self.logger.debug('Resolutions found are %s', grid_resolutions)
        return grid_resolutions

    @staticmethod
    def get_levelist(profile, local_grids, levels):
        """
        Get the level list from local grids.

        Args:
            profile (dict): Profile definition.
            local_grids (dict): Local grids definition.
            levels (dict): Levels definition.

        Returns:
            tuple: Levelist and levels values.
        """

        vertical = profile.get("vertical")
        if vertical is None:
            return None, None
        level_data = levels[local_grids[f"vertical-{vertical}"]]
        return level_data["levelist"], level_data["levels"]

    @staticmethod
    def get_time(frequency, levtype):
        """
        Get time string based on the frequency.

        Args:
            time_dict (dict): The time-related dictionary information

        Returns:
            str: Corresponding time string.
        """

        freq2time = {
            "hourly": {
                #'time': '"0000/to/2300/by/0100"',
                'time': '0000',
                'chunks': '6h' if levtype == 'pl' else 'D',
                'savefreq': 'h'
            },
            "6-hourly": {
                'time': '0000',
                'chunks': '6h',
                'savefreq': 'h'
            },
            "daily": {
                'time': "0000",
                'chunks': "D",
                'savefreq': "D"
            },
            "monthly": {
                'time': None,
                'chunks': 'MS',
                'savefreq': "MS"
            }
        }
        return freq2time[frequency]
    

    @staticmethod
    def get_value_from_map(value, value_map, value_type):
        """
        Get the value from the map based on the value type.
        """
        result = value_map.get(value)
        if not result:
            raise ValueError(f"Unexpected {value_type}: {value}")
        return result


    def load_jinja_template(self, template_file):
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
            self.logger.debug('Loading template for %s', template_file)
            return templateenv.get_template(os.path.basename(template_file))
        
        raise FileNotFoundError(f'Cannot file template file {template_file}')

    def get_profile_content(self, profile, grid_resolution):
        """
        Generate profile content based on the given parameters.

        Args:
            profile (dict): Profile definition.
            grid_resolution (str): Resolution value.

        Returns:
            dict: Generated profile content.
        """

        grid = self.local_grids[f"horizontal-{self.model.upper()}-{grid_resolution}"]
        
        aqua_grid = self.matching_grids[grid]
        levelist, levels_values = self.get_levelist(profile, self.local_grids, self.levels)

        levtype_str = (
            'atm2d' if profile["levtype"] == 'sfc' else
            'atm3d' if profile["levtype"] == 'pl' else
            'oce2d' if profile["levtype"] == 'o2d' else
            'oce3d' if profile["levtype"] == 'o3d' and 'full' in profile['vertical'] else
            'oce3d-half' if profile["levtype"] == 'o3d' and 'half' in profile['vertical'] else
            'sol4' if profile["levtype"] == 'sol' and profile['vertical'] == 'IFS-sol4' or profile['vertical'] == 'ICON-sol4' else
            'sol5' if profile["levtype"] == 'sol' and profile['vertical'] == 'IFS-sol5' or profile['vertical'] == 'ICON-sol5' else
            profile["levtype"] 
        )

        if not self.ocean_grid:
            self.ocean_grid = self.matching_grids['ocean_grid'][self.model][self.resolution]
            if self.ocean_grid is None:
                raise ValueError(f"No ocean grid available for: {self.model} {self.resolution}")

        if not self.atm_grid:
            self.atm_grid = self.matching_grids['atm_grid'][self.model][self.resolution]
            if self.atm_grid is None:
                raise ValueError(f"No atmospheric grid available for: {self.model} {self.resolution}")
                
        grid_mappings = self.matching_grids['grid_mappings']
        levtype = profile["levtype"]

        if levtype in grid_mappings:
            grid_str = grid_mappings[levtype].get(
                self.model, grid_mappings[levtype].get('default')).format(ocean_grid=self.ocean_grid, aqua_grid=aqua_grid)
        else:
            grid_str = grid_mappings['default'].format(aqua_grid=aqua_grid)
 
        source = f"{profile['frequency']}-{aqua_grid}-{levtype_str}"
        self.logger.info('Source: %s', source)

        self.logger.debug('levtype: %s, levels: %s, grid: %s', levtype, levelist, grid_str)

        time_dict = self.get_time(frequency=profile["frequency"], levtype=profile['levtype'])
        self.logger.debug('Time dict: %s', time_dict)
        self.logger.debug('Number of realizations %s', self.num_of_realizations)

        self.description = (
            self.config.get("description")
            or f'"{self.model} {self.config["exp"]} {self.config["data_start_date"][:4]}, '
            f'grids: {self.atm_grid} {self.ocean_grid}"' )
        
        # Set the stream based on the frequency
        stream = 'clmn' if profile['frequency'] == 'monthly' else 'clte'

        kwargs = {
            "dp_version": self.dp_version,
            "resolution": grid_resolution,
            "grid": grid_str,
            "source": source,
            "levelist": levelist,
            "num_of_realizations": self.num_of_realizations,
            "levels": levels_values,
            "levtype": profile["levtype"],
            "stream": stream,
            "variables": profile["variables"],
            "param": profile["variables"][0],
            "time": time_dict['time'],
            "chunks": time_dict['chunks'],
            "savefreq": time_dict['savefreq'], 
            "description": self.description
        }
        return kwargs

    def create_catalog_entry(self, all_content):
        """
        Create catalog entry file and update main YAML.

        Args:
            all_content (dict): Dictionary of all generated content strings.
        """
        output_dir = os.path.join(self.catalog_dir_path, 'catalogs',
                                  self.config['catalog_dir'], 'catalog', self.model.upper())
        os.makedirs(output_dir, exist_ok=True)
        output_filename = f"{self.config['exp']}.yaml"
        output_path = os.path.join(output_dir, output_filename)

        # Remove the existing file if it exists
        if os.path.exists(output_path):
            os.remove(output_path)

        # Write the catalog entry file
        dump_yaml(output_path, all_content)
        self.logger.info("File %s has been created in %s", output_filename, output_dir)

        # Update main.yaml
        main_yaml_path = os.path.join(output_dir, 'main.yaml')
        with SafeFileLock(main_yaml_path + '.lock', loglevel=self.loglevel):
            if not os.path.exists(main_yaml_path):
                main_yaml = {'sources': {}}
            else:
                main_yaml = load_yaml(main_yaml_path)

            resolution_map = {
                'production': 'HR',
                'develop': 'SR',
                'lowres': 'LR',
                'intermediate': 'MR'
            }
            
            resolution_id = self.get_value_from_map(self.config['resolution'], resolution_map, 'resolution')

            forcing_map = {
                'hist': 'historical',
                'cont': 'control',
                'SSP3-7.0': 'ssp370',
                'Tplus2.0K': 'tplus2K'
            }

            forcing = self.config.get('forcing')
            if not forcing:
                experiment = self.config['experiment']
                forcing = forcing_map.get(experiment, re.sub(r'[^a-z0-9]', '', experiment.lower()))
            
            main_yaml['sources'][self.config['exp']] = {
                'description': self.description,
                'metadata': {
                    'author': self.author,
                    'maintainer': self.config.get('maintainer') or 'not specified',
                    'machine': self.machine,
                    'expid': self.config['expid'],                
                    'resolution_atm': self.atm_grid,
                    'resolution_oce': self.ocean_grid,
                    'forcing': forcing,
                    'start': self.config['data_start_date'][:4], #year only
                    'dashboard': {
                        'menu': self.config.get('menu') or self.config['exp'],
                        'resolution_id': resolution_id,
                        'note': self.config.get('note')
                    }
                },
                'driver': 'yaml_file_cat',
                'args': {
                    'path': f"{{{{CATALOG_DIR}}}}/{self.config['exp']}.yaml"
                }
            }
            dump_yaml(main_yaml_path, main_yaml)
            self.logger.info("%s entry in 'main.yaml' has been updated in %s", self.config['exp'], output_dir)

        # Update catalog.yaml if a new model is added
        catalog_yaml_path = os.path.join(self.catalog_dir_path, 'catalogs',  self.config['catalog_dir'], 'catalog.yaml')
        with SafeFileLock(catalog_yaml_path + '.lock', loglevel=self.loglevel):
            catalog_yaml = load_yaml(catalog_yaml_path)

            if catalog_yaml.get('sources') is None:
                catalog_yaml['sources'] = {}

            if self.model not in catalog_yaml.get('sources', {}):  
                catalog_yaml.setdefault('sources', {}) 
                catalog_yaml['sources'][self.model.upper()] = {
                    'description': f"{self.model.upper()} model",
                    'driver': 'yaml_file_cat',
                    'args': {
                        'path': f"{{{{CATALOG_DIR}}}}/catalog/{self.model.upper()}/main.yaml"
                    }
                }
                dump_yaml(catalog_yaml_path, catalog_yaml)
                self.logger.info("%s entry in 'catalog.yaml' has been created at %s", self.model, catalog_yaml_path)


    def generate_catalog(self):
        """
        Generate the entire catalog by processing profiles and resolutions.
        """
        all_content = {'sources': {}}

        # Retrieve available resolutions for the current model
        self.grid_resolutions = self.get_available_resolutions(self.local_grids, self.model)
        
        if not self.grid_resolutions:
            self.logger.error('No resolutions found, generating an empty file!')
            return

        for profile in self.dp[self.model]:

            # Filter out omitted resolutions, if any
            current_resolutions = [
                res for res in self.grid_resolutions 
                if 'omit-resolutions' not in profile or res not in profile['omit-resolutions']
            ]

            for grid_resolution in current_resolutions:
        
                content = self.get_profile_content(profile, grid_resolution)
                combined = {**self.config, **content}
                source_name = combined.get('source')

                if source_name in all_content['sources']:
                    self.logger.debug('Source %s already exists, updating variables.', source_name)

                    source_content = all_content['sources'][source_name]
                    source_content['metadata']['variables'].extend(combined['variables'])

                    all_content['sources'][source_name] = source_content
                    self.logger.info('Added variables %s to source %s.', combined['variables'], source_name)
                else:
                    self.logger.debug('Creating catalog entry for %s.', source_name)
                    # Convert lists to inline format before rendering for better readability
                    for key in ['levels', 'variables']:
                        if key in combined and combined[key] is not None:
                            combined[key] = list(combined[key])
                    try:
                        rendered_content = yaml.load(self.template.render(combined))
                        all_content['sources'][source_name] = rendered_content[source_name]
                    except Exception as e:
                        self.logger.error('Error rendering template for source %s: %s', source_name, str(e))

        # Create final catalog entry
        self.create_catalog_entry(all_content)

def catgen_execute(args):
    """Useful wrapper for the FDB catalog generator class"""

    dp_version = get_arg(args, 'portfolio', 'full')
    config_file = get_arg(args, 'config', 'config.yaml')
    loglevel = get_arg(args, 'loglevel', 'INFO')

    generator = AquaFDBGenerator(dp_version, config_file, loglevel)
    generator.generate_catalog()


if __name__ == '__main__':

    args = catgen_parser().parse_args(sys.argv[1:])
    catgen_execute(args)


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
from aqua.util import load_yaml, dump_yaml, get_arg, ConfigPath
from aqua.logger import log_configure
from aqua.lra_generator.lra_util import replace_intake_vars

def catgen_parser(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(description='AQUA FDB entries generator')

    parser.add_argument("-p", "--portfolio", help="Type of Data Portfolio utilized (production/reduced)")
    parser.add_argument('-c', '--config', type=str, help='yaml configuration file', required=True)
    parser.add_argument('-l', '--loglevel', type=str, help='loglevel', default='INFO')

    return parser

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
        self.template = self.load_jinja_template(os.path.join(self.catgendir, f"{data_portfolio}.j2"))
        self.matching_grids = load_yaml(os.path.join(self.catgendir, "matching_grids.yaml"))

        # config options
        self.dp_dir_path = self.config["repos"]["data-portfolio_path"]
        self.catalog_dir_path = self.config["repos"]["Climate-DT-catalog_path"]
        self.model = self.config["model"].lower()
        #self.portfolio = self.config["portfolio"]
        self.resolution = self.config["resolution"]
        self.ocean_grid = self.config["ocean_grid"]
        self.num_of_realizations = int(self.config.get("num_of_realizations", 1))

        #sefaty check
        if (data_portfolio == 'production' and self.resolution not in ['production', 'lowres', 'develop'] or
            data_portfolio == 'reduced' and self.resolution not in ['intermediate']):
            raise KeyError(f'Wrong match between data portfolio {data_portfolio} and data resolution {self.resolution}')

        # portfolio
        self.logger.info("Running FDB catalog generator for %s portfolio for model %s", data_portfolio, self.model)
        self.dp = load_yaml(os.path.join(self.dp_dir_path, data_portfolio, 'portfolio.yaml'))
        self.grids = load_yaml(os.path.join(self.dp_dir_path, data_portfolio, 'grids.yaml'))
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
            "daily": {
                'time': "0000",
                'chunks': "D",
                'savefreq': "D"
            },
            "monthly": {
                'time': None,
                'chunks': 'D' if levtype == 'o3d' else 'MS',
                'savefreq': "MS"
            }
        }
        return freq2time[frequency]

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
            'sol4' if profile["levtype"] == 'sol' and profile['vertical'] == 'sol4' else
            'sol5' if profile["levtype"] == 'sol' and profile['vertical'] == 'sol5' else
            profile["levtype"]
        )

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

        kwargs = {
            "dp_version": self.dp_version,
            "resolution": grid_resolution,
            "grid": grid_str,
            "source": source,
            "levelist": levelist,
            "num_of_realizations": self.num_of_realizations,
            "levels": levels_values,
            "levtype": profile["levtype"],
            "variables": profile["variables"],
            "param": profile["variables"][0],
            "time": time_dict['time'],
            "chunks": time_dict['chunks'],
            "savefreq": time_dict['savefreq'], 
        }
        return kwargs

    def create_catalog_entry(self, all_content):
        """
        Create catalog entry file and update main YAML.

        Args:
            all_content (list): List of all generated content strings.
        """
        output_dir = os.path.join(self.catalog_dir_path, 'catalogs',
                                  self.config['catalog_dir'], 'catalog', self.model.upper())
        os.makedirs(output_dir, exist_ok=True)
        output_filename = f"{self.config['exp']}.yaml"
        output_path = os.path.join(output_dir, output_filename)

        if os.path.exists(output_path):
            os.remove(output_path)

        with open(output_path, "w", encoding='utf8') as f:
            f.write('sources:\n')
            for content in all_content:
                f.write(f'  {content}\n')

        self.logger.info("File %s has been created in %s", output_filename, output_dir)

        main_yaml_path = os.path.join(output_dir, 'main.yaml')
        if not os.path.exists(main_yaml_path):
            main_yaml = {'sources': {}}
        else:
            main_yaml = load_yaml(main_yaml_path)
        main_yaml['sources'][self.config['exp']] = {
            'description': self.config['description'],
            'driver': 'yaml_file_cat',
            'args': {
                'path': f"{{{{CATALOG_DIR}}}}/{self.config['exp']}.yaml"
            }
        }
        dump_yaml(main_yaml_path, main_yaml)
        self.logger.info("%s entry in 'main.yaml' has been updated in %s", self.config['exp'], output_dir)

    def generate_catalog(self):
        """
        Generate the entire catalog by processing profiles and resolutions.
        """

        all_content = []

        grid_resolutions = self.get_available_resolutions(self.local_grids, self.model)
        for profile in self.dp[self.model]:
            if not grid_resolutions:
                self.logger.error('No resolutions found, generating an empty file!')
            for grid_resolution in grid_resolutions:
                content = self.get_profile_content(profile, grid_resolution)
                combined = {**self.config, **content}
                self.logger.debug('Creating catalog entry for %s', combined['source'])
                #self.logger.debug(combined)
                for replacepath in ['fdb_home', 'fdb_home_bridge']:
                    if 'replacepath' in combined:
                        combined[replacepath] = '"' + replace_intake_vars(combined[replacepath], catalog=combined['catalog_dir']) + '"'
                all_content.append(self.template.render(combined))

        self.create_catalog_entry(all_content)

def catgen_execute(args):

    """Useful wrapper for the FDB catalog generator class"""

    dp_version = get_arg(args, 'portfolio', 'production')
    config_file = get_arg(args, 'config', 'config.yaml')
    loglevel = get_arg(args, 'loglevel', 'INFO')

    generator = AquaFDBGenerator(dp_version, config_file, loglevel)
    generator.generate_catalog()

if __name__ == '__main__':

    args = catgen_parser().parse_args(sys.argv[1:])
    catgen_execute(args)


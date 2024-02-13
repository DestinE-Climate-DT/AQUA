from ruamel.yaml import YAML
import os
from aqua.util import ConfigPath

yaml = YAML()
yaml.representer.ignore_aliases = lambda *args: True

# Read variables from config file
with open('config.yaml', 'r') as config_file:
    config = yaml.load(config_file)

# Common dictionary
common_dict = {
    'args': {
        'request': {
            'domain': 'g',
            'class': 'rd',
            'expver': config['expver'],
            'type': 'fc',
            'stream':'',
            'date': config['date'],
            'time': '0000',
            'param': '',
            'levtype': 'sfc',
            'step': 0
        },
        'data_start_date': config['data_start_date'],
        'data_end_date': config['data_end_date'],
        'aggregation': '',
        'savefreq': '',
        'timestep': 'H',
        'timestyle': 'step'},
    'driver': 'gsv',
    'metadata': {
        'fdb_path': config['fdb_path'],
        'eccodes_path': config['eccodes_path'],
        'source_grid_name': config['grid'],
        'variables': ''
        }
}            

# Sources dictionary

sources = {
    'hourly-native': {
        **common_dict,
        'args': {
            **common_dict['args'],
            'request': {**common_dict['args']['request'],
                    'stream': 'lwda',
                    'param': '2t'}, 
            'data_start_date': common_dict['args']['data_start_date'] + 'T0000',
            'data_end_date': common_dict['args']['data_end_date'] + 'T2300',
            'aggregation': 'D',
            'savefreq': 'h'
        }
    },
    'hourly-1deg': {
        **common_dict,        
        'args': {
            **common_dict['args'],
            'request': {**common_dict['args']['request'],
                    'stream': 'scda',
                    'param': 'sf'},
            'data_start_date': common_dict['args']['data_start_date'] + 'T0000',
            'data_end_date': common_dict['args']['data_end_date'] + 'T2300',
            'aggregation': 'D',
            'savefreq': 'h'
        }
    },
    '6hourly-1deg': {
        **common_dict,
        'args': {
             **common_dict['args'],
        'request': {**common_dict['args']['request'],
                'stream': 'scwv', 
                'param': 'z',
                'levtype': 'pl', 
                'levelist': [1, 5, 10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000],
                'step': 6},
        'data_start_date': common_dict['args']['data_start_date'] + 'T0000',
        'data_end_date': common_dict['args']['data_end_date'] + 'T1800',
        'aggregation': 'D',
        'savefreq': '6h'
        }
    },           

    'monthly-1deg-2d': {
        **common_dict,
    'args': {
        **common_dict['args'],
        'request': {**common_dict['args']['request'], 
                'stream': 'monr',
                'param': 'sd'},
        'data_end_date': common_dict['args']['data_end_date'][:-2] + '01',
        'aggregation': 'Y',
        'savefreq': 'M'
        }
    },
    'monthly-1deg-3d': {
        **common_dict,
    'args': {
        **common_dict['args'],
        'request': {**common_dict['args']['request'], 
                'stream': 'monr',
                'param': 't',
                'levtype': 'pl', 
                'levelist': [1, 5, 10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]},
        'data_end_date': common_dict['args']['data_end_date'][:-2] + '01',
        'aggregation': 'M',
        'savefreq': 'M',
        'timeshift': 'Yes'
        }
    }
}

configurer = ConfigPath()
catalog_path, fixer_folder, config_file = configurer.get_reader_filenames()

#create output file in model folder
output_filename = f"{config['exp']}.yaml"
folder_path = os.path.join(os.path.dirname(catalog_path), 'catalog', config['model'])
output_file_path = os.path.join(folder_path, output_filename)
with open(output_file_path, 'w') as output_file:
    yaml.preserve_quotes ==True
    yaml.dump({'sources': sources}, output_file)


print(f"File '{output_filename}' has been created in '{folder_path}'.")

#update main.yaml

main_yaml_path = os.path.join(folder_path, 'main.yaml')

with open(main_yaml_path, 'r') as main_file:
    main_yaml = yaml.load(main_file)

main_yaml['sources'][config['exp']] = {
    'description': config['description'],
    'driver': 'yaml_file_cat',
    'args': {
        'path': f"{{{{CATALOG_DIR}}}}/{config['exp']}.yaml"
    }
}

with open(main_yaml_path, 'w') as main_file:
    yaml.dump(main_yaml, main_file)

print(f"'exp' entry in 'main.yaml' has been updated in '{folder_path}'.")

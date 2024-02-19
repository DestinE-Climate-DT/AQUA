import jinja2
import os
from aqua.util import ConfigPath, load_yaml, dump_yaml

definitions = load_yaml('config.tmpl')

#eccodes path
definitions['eccodes_path'] = '/projappl/project_465000454/jvonhar/aqua/eccodes/eccodes-' + definitions['eccodes_version'] + '/definitions'

#levels for IFS-NEMO:
if definitions['model'] == 'IFS-NEMO':  
    definitions['ifs_levels'] = [1, 5, 10, 20, 30, 50, 70, 100, 150, 200, 250, 300, 400, 500, 600, 700, 850, 925, 1000]
    definitions['nemo_levellist'] = list(range(1,76))
    definitions['nemo_levels'] =  [0.5057600140571594, 1.5558552742004395, 2.6676816940307617, 3.8562798500061035,
        5.140361309051514, 6.543033599853516, 8.09251880645752, 9.822750091552734,
        11.773679733276367, 13.99103832244873, 16.52532196044922, 19.42980194091797,
        22.75761604309082, 26.558300018310547, 30.874561309814453, 35.740203857421875,
        41.180023193359375, 47.21189498901367, 53.85063552856445, 61.11283874511719,
        69.02168273925781, 77.61116027832031, 86.92942810058594, 97.04131317138672,
        108.03028106689453, 120.0, 133.07582092285156, 147.40625, 163.16445922851562,
        180.5499267578125, 199.7899627685547, 221.14117431640625, 244.890625, 271.35638427734375,
        300.88751220703125, 333.8628234863281, 370.6884765625, 411.7938537597656,
        457.6256103515625, 508.639892578125, 565.2922973632812, 628.0260009765625,
        697.2586669921875, 773.3682861328125, 856.678955078125, 947.4478759765625,
        1045.854248046875, 1151.9912109375, 1265.8614501953125, 1387.376953125, 1516.3636474609375,
        1652.5684814453125, 1795.6707763671875, 1945.2955322265625, 2101.026611328125,
        2262.421630859375, 2429.025146484375, 2600.38037109375, 2776.039306640625,
        2955.5703125, 3138.56494140625, 3324.640869140625, 3513.445556640625, 3704.65673828125,
        3897.98193359375, 4093.15869140625, 4289.95263671875, 4488.15478515625, 4687.5810546875,
        4888.06982421875, 5089.478515625, 5291.68310546875, 5494.5751953125, 5698.060546875,
        5902.0576171875]


# jinja2 loading and replacing (to be checked)
templateLoader = jinja2.FileSystemLoader(searchpath='./')
templateEnv = jinja2.Environment(loader=templateLoader, trim_blocks=True, lstrip_blocks=True)


template = templateEnv.get_template('ifs-nemo-catalog.j2')
outputText = template.render(definitions)


#create output file in model folder
configurer = ConfigPath()
catalog_path, fixer_folder, config_file = configurer.get_reader_filenames()

output_dir = os.path.join(os.path.dirname(catalog_path), 'catalog', definitions['model'])
output_filename = f"{definitions['exp']}.yaml"
output_path = os.path.join(output_dir, output_filename)

with open(output_path, "w", encoding='utf8') as output_file:
    output_file.write(outputText)

print(f"File '{output_filename}' has been created in '{output_dir}'.")

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

print(f"'exp' entry in 'main.yaml' has been updated in '{output_dir}'.")
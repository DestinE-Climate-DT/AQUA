# Importing the aqua.slurm module and slurm supporting functions nedeed for your script
from aqua.slurm import slurm 
from config.slurm_supporting_func import get_job_status, waiting_for_slurm_response

# All nesessarry import for a cli diagnostic 
####################################################################
import sys
from aqua import Reader
sys.path.insert(0, '../../')
from tropical_rainfall import Tropical_Rainfall
####################################################################


# Loading the data from yml file 
####################################################################
import yaml
with open('config/trop_rainfall_config.yml', 'r') as file:
    config = yaml.safe_load(file)

trop_lat        = config['class_attributes']['trop_lat']
num_of_bins     = config['class_attributes']['num_of_bins']
first_edge      = config['class_attributes']['first_edge']
width_of_bin    = config['class_attributes']['width_of_bin']

model_variable  = config['model_variable'] 
new_unit        = config['new_unit']

path_to_netcdf  = config['path']['path_to_netcdf']
path_to_pdf     = config['path']['path_to_pdf']


model           = config['data']['model']
exp             = config['data']['exp']
source          = config['data']['source']
####################################################################

# Job initialization 
slurm.job()

waiting_for_slurm_response(10)

for i in range(0, 60):
    if get_job_status() == 'R':
        print('The job is started to run!')
        ##############################################################
        reader          = Reader(model = model, exp = exp, source = source)
        data            = reader.retrieve()

        diag            = Tropical_Rainfall(trop_lat = trop_lat,       num_of_bins = num_of_bins, 
                                            first_edge = first_edge,   width_of_bin = width_of_bin)

        diag.histogram(data, path_to_histogram = path_to_netcdf,        name_of_file="TEST_CLI")
        ##############################################################
        break
    else:
        print('The job is waiting in the queue')
        waiting_for_slurm_response(60)

# Note: The loop will stop to check your job status only for one hour. If the queue is busy, 
# consider increasing the range of your loop.
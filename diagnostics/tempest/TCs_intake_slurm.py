import sys
sys.path.append('../../')

from functions_TCs import detect_nodes_zoomin, stitch_nodes_zoomin


# path to directories

# put the dirs in a yaml file
dirs ={ 'regdir':'/home/b/b382216/scratch/regrid_intake',
        'tmpdir':'/home/b/b382216/scratch/tmpdir_intake',
        'fulldir':'/home/b/b382216/scratch/fullres'}

# dimension (in degrees) of the box to be saved
boxdim=10

# resolution for high and low res data
lowgrid='r100'
highgrid='r100'

# possibly put this in the fixer
# variables to be stored
varlist = ['psl', 'uas', 'vas', 'pr']

# dicitonary with the original filenames
original_dictionary = {'psl': 'msl', 'uas': '10u', 'vas': '10v', 'pr': 'tp'}

# experiment details
model='IFS'
experiment_name = 'tco2559-ng5'

#initial year and month
init_year=2020
init_month=7
init_day=1

#final year and month
end_year=2020
end_month=9
end_day=30

# support variables
hour_freq = 6 #hourly frequency to retrieve data
retrieve_frequency = str(hour_freq)+'h' 

# keyword argument or define a class 
# dicitonary with the original filenames
retrieve_dictionary = {'init_year': init_year, 'init_month': init_month, "init_day": init_day, 
                       'end_year': end_year, 'end_month': end_month, "end_day": end_day,
                       'frequency': retrieve_frequency,
                       'model': model, 'exp': experiment_name}

# At first run DetectNodes then save NETCDF files with variables with data only in a box in the vicinity of the TCs centres

detect_nodes_zoomin(retrieve_dictionary=retrieve_dictionary, dirs=dirs, varlist=varlist, \
                    original_dictionary=original_dictionary, lowgrid=lowgrid, highgrid=highgrid, boxdim=boxdim, write_fullres=True)

# n_days_ext = number of days in which each time block is extended at the beginning and at the end
# write_fullres = whether or not store fullres netcdf files
# n_days_freq = number of days to save in the NETCDF file

stitch_nodes_zoomin(retrieve_dictionary=retrieve_dictionary, dirs=dirs, varlist=varlist, \
                    boxdim=boxdim, n_days_ext = 5, n_days_freq = 30, write_fullres=True)
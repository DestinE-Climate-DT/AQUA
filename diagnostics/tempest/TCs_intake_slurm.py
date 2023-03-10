import sys
sys.path.append('../../')
import os
from datetime import datetime, timedelta
from glob import glob

from dask_jobqueue import SLURMCluster
from dask.distributed import Client
import dask
import matplotlib as mpl
# Define Agg as Backend for matplotlib when no X server is running
mpl.use('Agg')
import matplotlib.pyplot as plt
import socket

import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)

def squeue_user(username = "ghinassi"):
    _squeue_user = os.system("squeue --user="+str(username))
    return _squeue_user 

with os.popen("pwd ") as f:
    _pwd = f.readline()


squeue_user()
print("my file in folder " + str(_pwd))

extra_args=[
    "--exclusive",
    "--get-user-env", "--verbose",
    "--error="+str(_pwd)+"/slurm_output/logs/dask-worker-%j.err",
    "--account=nnazarova",
    "--output="+str(_pwd)+"/slurm_output/dask-worker-%j.out"
]

cluster = SLURMCluster(
    name='dask-cluster', 
    cores=1,                        # Number of cores per job
    memory=f"{8 * 64 * 0.90} MB",   # Amount of memory per job
    processes=6,                    # Number of Python processes to cut up each job
    project="bm1235",
    queue="batch",
    #interface='eth0', #'ib0',
    walltime='00:10:00',
    job_extra=extra_args,
    env_extra=["module load python3",
                "cd "+str("+str(_pwd)+")],
)
client = Client(cluster)
print(cluster.job_script())

cluster.scale(jobs=1)
cluster.close()


from functionsTCs import *

from aqua.reader import catalogue
catalogue(catalog_file='../../config/catalog.yaml');

# path to input directory
regdir='/home/b/b382216/scratch/regrid_intake'
tmpdir='/home/b/b382216/scratch/tmpdir_intake'
fulldir='/home/b/b382216/scratch/fullres'

# dimension of the box to be saved
boxdim=10

# resolution for high and low data
lowgrid='r100'
highgrid='r100'

# variables to be stored
varlist = ['psl', 'uas', 'vas', 'pr']
#varlist = ['pr']

# dicitonary with the original filenames
original_dictionary = {'psl': 'msl', 'uas': '10u', 'vas': '10v', 'pr': 'tp'}

# ndays to be saved
ndays = 90

#initial year and month
init_year=2020
init_month=7
init_day=1

# timestep to run on (depends on the experiment)
t1=0
t2=6*4*ndays

# initial date from which start detection/tracking
initial_date=datetime(init_year, init_month, init_day, 0, 0, 0)

# At first run DetectNodes then save NETCDF files with variables with data only in a box in the vicinity of the TCs centres

# loop on timerecords
for t in range(t1, t2, 6): 

    tttt = initial_date + timedelta(hours=t)
    tstep = tttt.strftime('%Y%m%dT%H')
    print(tstep)
    # read from catalog, interpolate, write to disk and create a dictionary with useful information
    tempest_dictionary = readwrite_from_intake(model='IFS', exp = 'tco2559-ng5', timestep=tttt, grid=lowgrid, tgtdir=regdir)

    # define the tempest detect nodes output
    txt_file = os.path.join(tmpdir, 'tempest_output_' + tstep + '.txt')

    # run the node detection on the low res files
    tempest_command = run_detect_nodes(tempest_dictionary, tempest_dictionary['regrid_file'], txt_file)

    # remove the low res files
    clean_files([tempest_dictionary['regrid_file']])
    
    # identify the nodes
    tempest_nodes = read_lonlat_nodes(txt_file)

    # load the highres files
    #reader2d = Reader(model='IFS', exp = 'tco2559-ng5', source="ICMGG_atm2d")
    reader2d = Reader(model='IFS', exp = 'tco2559-ng5', source="ICMGG_atm2d", regrid=highgrid)
    fulldata = reader2d.retrieve().sel(time=tstep)
    
    # loop on variables to write to disk only the subset of high res files
    for var in varlist : 

        varfile = original_dictionary[var]

        data = reader2d.regrid(fulldata[varfile])
        data.name = var
        xfield = store_fullres_field(0, data, tempest_nodes, boxdim)

        store_file = os.path.join(tmpdir, f'TC_{var}_{tstep}.nc')
        write_fullres_field(xfield, store_file)
        
        
# Then run StitchNodes to remove spurious tracks and write NETCDF files with var every n days
        
#number of days in which each month is extended at the beginning and at the end
n_days_ext = 10
# number of days to save in the NETCDF file
n_days_freq = 30

# support variables
frequency = str(n_days_freq)+'D'
real_end_month = end_month + 1

# loop to run stitch nodes and save NETCDF files every n days

for block in pd.date_range(start=f'{init_year}-{init_month}-{init_day}', end=f'{end_year}-{real_end_month}', freq=frequency):

    # create DatetimeIndex with daily frequency
    end_day = calendar.monthrange(block.year, block.month)[1]
    dates = pd.date_range(start=block, periods=n_days_freq, freq='D')

    before = dates.shift(-n_days_ext, freq='D')[0:n_days_ext]
    after = dates.shift(+n_days_ext, freq='D')[-n_days_ext:]

    # concatenate the indexes to create a single index
    date_index = before.append(dates).append(after)

    # create list of file paths to include in glob pattern
    file_paths = [os.path.join(tmpdir, f'tempest_output_{date}T??.txt') for date in date_index.strftime('%Y%m%d')]
    # use glob to get list of filenames that match the pattern
    filenames = []
    for file_path in file_paths:
        filenames.extend(sorted(glob.glob(file_path)))
    print(filenames)

    track_file = os.path.join(tmpdir, f'tempest_track_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.txt')

    # run stitch nodes, MAXGAP set to 6h to match the input files res
    stitch_string = run_stitch_nodes(filenames, track_file, maxgap = '6h')
    
    # create DatetimeIndex with daily frequency
    end_day = calendar.monthrange(block.year, block.month)[1]
    dates = pd.date_range(start=block, periods=n_days_freq, freq='D')
    
    # create output file with output from stitch nodes 
    track_file = os.path.join(tmpdir, f'tempest_track_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.txt')

    # reordered_tracks is a dict containing the concatenated (in time) tracks
    # at eatch time step are associated all lons/lats

    reordered_tracks = reorder_tracks(track_file)

    # initialise full_res fields at 0 before the loop
    
    for var in varlist : 
        print(var)

        xfield = 0
        for idx in reordered_tracks.keys():
            #print(datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d'))
            #print (dates.strftime('%Y%m%d'))
            if datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%d') in dates.strftime('%Y%m%d'):

                timestep = datetime.strptime(idx, '%Y%m%d%H').strftime('%Y%m%dT%H')
                
                fullres_file = os.path.join(tmpdir, f'TC_{var}_{timestep}.nc')
                fullres_field = xr.open_mfdataset(fullres_file)[var]

                # get the full res field and store the required values around the Nodes
                xfield = store_fullres_field(xfield, fullres_field, reordered_tracks[idx], boxdim)

        print('Storing output')

        # store the file
        store_file = os.path.join(tmpdir, f'tempest_tracks_{var}_{block.strftime("%Y%m%d")}-{dates[-1].strftime("%Y%m%d")}.nc')
        write_fullres_field(xfield, store_file)
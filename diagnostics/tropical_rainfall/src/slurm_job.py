import os 
import re

from dask_jobqueue import SLURMCluster # pip 
from dask.distributed import Client, progress 


#list of functions 
    # squeue_user 
    # pwd
    # slurm_interactive_job
    #scancel

def squeue_user(username = "$USER"):
    _squeue_user = os.system("squeue --user="+str(username))
    return _squeue_user 

def pwd():
    with os.popen("pwd ") as f:
        _pwd = f.readline()
    return re.split(r'[\n]', _pwd)[0]
    

def slurm_interactive_job(cores=1, memory="100 GB", queue = "compute", walltime='04:30:50', jobs=1):
    extra_args=[
        "--error="+str(pwd())+"/slurm/logs/dask-worker-%j.err",
        "--output="+str(pwd())+"/slurm/output/dask-worker-%j.out"
    ]

    cluster = SLURMCluster(
        name='dask-cluster', 
        cores=cores,    
        memory=memory, 
        project="bb1153",
        queue=queue, 
        walltime=walltime,
        job_extra=extra_args,
    )
    client = Client(cluster)
    print(cluster.job_script())
    cluster.scale(jobs=jobs)

def scancel(Job_ID=None):
    os.system("scancel " +str(Job_ID)) 
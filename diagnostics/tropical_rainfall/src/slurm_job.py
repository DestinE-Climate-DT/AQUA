import os 
import re

from dask_jobqueue import SLURMCluster # pip 
from dask.distributed import Client, progress 

"""
list of functions 
    1) squeue_user 
    2) pwd
    3) slurm_interactive_job
    4) scancel
"""


def squeue_user(username = "$USER"):
    """
    Arguments:
       username:        (str)       :  Username of user in a slurm workload manager. 
                                        By default, username = "$USER", which is valid in case, if USER who running the ```squeue_user``` function is the same user, who runnng job    
    """
    _squeue_user = os.system("squeue --user="+str(username))
    return _squeue_user 

def pwd():
    """
    """
    with os.popen("pwd ") as f:
        _pwd = f.readline()
    return re.split(r'[\n]', _pwd)[0]
    

def slurm_interactive_job(cores=1, memory="100 GB", queue = "compute", walltime='04:30:50', jobs=1):
    """
    """
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
    """

    """
    os.system("scancel " +str(Job_ID)) 
import os 
import re

from dask_jobqueue import SLURMCluster # pip 
from dask.distributed import Client, progress 

"""The module contains functions to create and control the SLURM job:
     - squeue,
     - job,
     - output_dir,
     - scancel,
     - max_resources_per_node

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>

"""
def squeue(username = "$USER"):
    """Checking the status of a SLURM job

    Args:
        username (str, optional): Name of the user who submitted the job to the queue. Defaults to "$USER".

    Returns:
        str:   The status of all jobs of the user in a SLURM queue
    """
    squeue_user = os.system("squeue --user="+str(username))
    return squeue_user 


    

def output_dir(path_to_output = '.'):
    """Creating the directory for output if it does not exist

    Args:
        path_to_output (str, optional): The path to the directory, which will contain logs/errors and output of Slurm Jobs. Defaults to '.'

    Returns:
        str: The path to the directory for logs/errors
        str: The path to the directory for output 
    """    
    logs_path = str(path_to_output)+"/slurm/logs"
    output_path = str(path_to_output)+"/slurm/output"

    if not os.path.exists(str(path_to_output)+"/slurm"):
        os.makedirs(str(path_to_output)+"/slurm")
    if not os.path.exists(str(path_to_output)+"/slurm/logs"):  
        os.makedirs(logs_path) 
    if not os.path.exists(str(path_to_output)+"/slurm/output"):   
        os.makedirs(output_path)    
    
    return logs_path, output_path


def exctract_sinfo(sinfo_str=' '):
    """_summary_

    Args:
        sinfo_str (str, optional): _description_. Defaults to ' '.

    Returns:
        _type_: _description_
    """    
    with os.popen(sinfo_str) as f:
            f.readline()
            f.readline()
            _ = f.readline()
    list_with_empty_strings= re.split(r'[\n ]', _)
    new_list = [x for x in list_with_empty_strings if x != '']
    return new_list
     
def max_resources_per_node(queue="compute"):
    """_summary_

    Args:
        queue (str, optional): _description_. Defaults to "compute".

    Raises:
        Exception: _description_
        Exception: _description_

    Returns:
        _type_: _description_
    """    
    max_resources = exctract_sinfo("sinfo  --partition="+str(queue)+" -lNe")
    if max_resources[2]==queue:   
        max_cpus = max_resources[4]
        max_memory  = str (float(max_resources[6])/1024)+" GB"
        max_sockets, max_cores, max_threads = re.split(r'[:]', max_resources[5])
    else:
        raise Exception("The function can not extract information about the queue correctly. \n \
                        Please, select the amount of memory, cores, threads, and walltime manually.")

    if queue == exctract_sinfo("sinfo  --partition="+str(queue)+" -le")[0]:
        max_walltime = exctract_sinfo("sinfo  --partition="+str(queue)+" -le")[2]
    else:
        raise Exception("The function can not extract information about the queue correctly. \n \
                        Please, select the amount of memory, cores, threads, and walltime manually.")
    
    return  max_memory, max_walltime, max_cpus, max_sockets, max_cores, max_threads 


def job(exclusive = False, max_resources = False, cores=1, memory="10 GB", queue = "compute", walltime='02:30:00', jobs=1, account="bb1153", path_to_output='.'):
    """Submitting the Job to the SLURM queue 

    Args:
        cores (int, optional):      The number of cores per socket. Defaults to 1.
        memory (str, optional):     The real memory required per node. Defaults to "10 GB".
        queue (str, optional):      The name of the queue to which SLURM submits the job. Defaults to "compute".
        walltime (str, optional):   The duration for which the nodes remain allocated to you. Defaults to '02:30:00'.
        jobs (int, optional):       The factor of assignment scaling across multiple nodes. Defaults to 1.
    """

    
    logs_path, output_path = output_dir(path_to_output=path_to_output)

    if exclusive:
        extra_args=[
            "--error="+str(logs_path)+"/dask-worker-%j.err",
            "--output="+str(output_path)+"/dask-worker-%j.out",
            "--get-user-env",
            "--exclusive"
        ]
    else:
        extra_args=[
            "--error="+str(logs_path)+"/dask-worker-%j.err",
            "--output="+str(output_path)+"/dask-worker-%j.out"
        ]
        
    if max_resources:
        memory, walltime, cores, sockets_per_node, cores_per_socket, threads_per_core   = max_resources_per_node(queue = queue)
        cores = int(cores)
    
    cluster = SLURMCluster(
        name='dask-cluster', 
        cores=cores,    
        memory=memory, 
        account=account,
        queue=queue, 
        walltime=walltime,
        job_extra_directives=extra_args,
    )
    
    client = Client(cluster)
    print(cluster.job_script())
    cluster.scale(jobs=jobs)

def scancel(all = True, Job_ID=None):
    """scancel() is used to signal or cancel jobs in the queue

    Args:
        all (bool, optional):       If all is True, the function cancels all user jobs in the queue.
        Job_ID (str, optional):     The SLURM_JOB_ID of a job to cancel in the queue. Defaults to None.
    """
    if Job_ID==None:
        if all:
            os.system("scancel -u ${USER}")
    else:
        os.system("scancel " +str(Job_ID)) 

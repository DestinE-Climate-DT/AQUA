import os
import re

from dask_jobqueue import SLURMCluster  # pip
from dask.distributed import Client
from aqua.logger import log_configure
from aqua.util import create_folder, ConfigPath

"""
The Slurm module contains functions to create and control the SLURM job:
     - squeue,
     - job,
     - output_dir,
     - scancel,
     - max_resources_per_node

.. moduleauthor:: AQUA team <natalia.nazarova@polito.it>
"""


def squeue(username="$USER", verbose=True):
    """
    Checking the status of a SLURM job

    Args:
        username (str, optional): Name of the user who submitted
                                  the job to the queue.
                                  Defaults to "$USER".
        verbose (bool, optional): If True, more info are printed.

    Returns:
        squeue_user (str):   The status of all jobs of the user
                             in a SLURM queue
    """
    if verbose:
        squeue_user = os.system(
            "squeue --user=" +
            str(username) +
            " --format=\"%10i %5C %5D %10t %20j %10M %20S %20E %20P %20m\"")
    else:
        squeue_user = os.system("squeue --user=" + str(username))
    return squeue_user


def output_dir(path_to_output='.', loglevel='WARNING'):
    """
    Creating the directory for output if it does not exist

    Args:
        path_to_output (str, optional): The path to the directory,
                                        which will contain logs/errors and
                                        output of Slurm Jobs. Defaults is '.'
        loglevel (str, optional):       The level of logging.
                                        Defaults to 'WARNING'.

    Returns:
        logs_path (str):    The path to the directory for logs/errors
        output_path (str):  The path to the directory for output
    """
    logs_path = str(path_to_output) + "/slurm/logs"
    output_path = str(path_to_output) + "/slurm/output"

    # Creating the directory for logs and output
    create_folder(folder=str(path_to_output) + "/slurm", loglevel=loglevel)
    create_folder(folder=logs_path, loglevel=loglevel)
    create_folder(folder=output_path, loglevel=loglevel)

    return logs_path, output_path


def exctract_sinfo(sinfo_str=None):
    """
    Extracting the information about the queue

    Args:
        sinfo_str (str):   String with the info to extract.

    Returns:
        new_list (list):    List with the extracted info
    """

    if sinfo_str:
        with os.popen(sinfo_str) as f:
            f.readline()
            f.readline()
            _ = f.readline()
        list_with_empty_strings = re.split(r'[\n ]', _)
        new_list = [x for x in list_with_empty_strings if x != '']
    else:
        raise ValueError("sinfo_str is None")

    return new_list


def max_resources_per_node(queue="compute"):
    """
    Extracting the maximum resources available on the node for the queue

    Args:
        queue (str, optional):  The name of the queue to which maximum resources
                                available on the node are extracted.
                                Defaults to "compute".

    Returns:
        max_memory (str):       The maximum amount of memory available on the
                                node for the queue
        max_walltime (str):     The maximum amount of walltime available on the
                                node for the queue
        max_cpus (str):         The maximum number of cpus available on the
                                node for the queue
        max_sockets (str):      The maximum number of sockets available on the
                                node for the queue
        max_cores (str):        The maximum number of cores available on the
                                node for the queue
        max_threads (str):      The maximum number of threads available on the
                                node for the queue
    """
    max_resources = exctract_sinfo("sinfo  --partition=" + str(queue) + " -lNe")

    if max_resources[2] == queue:
        max_cpus = max_resources[4]
        max_memory = str(float(max_resources[6]) / 1024) + " GB"
        max_sockets, max_cores, max_threads = re.split(r'[:]',
                                                       max_resources[5])
    else:
        raise Exception("The function can not extract information about the queue correctly. \n \
                        Please, select the amount of memory, cores, threads, and walltime manually.")

    if queue == exctract_sinfo("sinfo  --partition=" + str(queue) + " -le")[0]:
        max_walltime = exctract_sinfo("sinfo  --partition=" + str(queue) + " -le")[2]
    else:
        raise Exception("The function can not extract information about the queue correctly. \n \
                        Please, select the amount of memory, cores, threads, and walltime manually.")

    return max_memory, max_walltime, max_cpus, max_sockets, \
        max_cores, max_threads


def job(exclusive=False, max_resources=False, cores=1, memory="10 GB",
        configdir=None, queue=None, account=None, walltime='02:30:00',
        jobs=1, path_to_output='.', loglevel='WARNING', machine=None):
    """
    Submitting the Job to the SLURM queue

    Args:
        exclusive (bool, optional):     If True, the job will be submitted
                                        asking for exclusive access to the
                                        node. Defaults to False.
        max_resources (bool, optional): If True, the job will be submitted
                                        asking for the maximum resources
                                        available on the node. Defaults to
                                        False.
        cores (int, optional):          The number of cores per socket.
                                        Defaults to 1.
        memory (str, optional):         The real memory required per node.
                                        Defaults to "10 GB".
        configdir (str, optional):      The path to the directory with the
                                        configuration files.
        queue (str, optional):          The name of the queue to which SLURM
                                        submits the job.
                                        Defaults to None.
        walltime (str, optional):       The duration for which the nodes remain
                                        allocated to you.
                                        Defaults to '02:30:00'.
        jobs (int, optional):           The factor of assignment scaling across
                                        multiple nodes. Defaults to 1.
        account (str, optional):        The account to which SLURM charges the
                                        job. Defaults to None.
        path_to_output (str, optional): The path to the directory,
                                        which will contain logs/errors and
                                        output of Slurm Jobs. Defaults is '.'
        loglevel (str, optional):       The level of logging.
                                        Defaults to 'WARNING'.
        machine (str, optional):        The name of the machine.
                                        Defaults to None.
    """
    # Initializing the logger
    logger = log_configure(log_level=loglevel, log_name='slurm')

    # Getting the machine name
    if machine is None:
        Configurer = ConfigPath(configdir=configdir)
        machine_name = Configurer.machine
    else:
        machine_name = machine

    # Setting default values for the queue and account
    if queue is None:
        if machine_name == "levante":
            queue = "compute"
        elif machine_name == "lumi":
            queue = "small"
        else:
            raise Exception("The queue is not defined. Please, define the queue manually.")
    if account is None:
        if machine_name == "levante":
            account = "bb1153"
        elif machine_name == "lumi":
            account = "project_465000454"
        else:
            raise Exception("The account is not defined. Please, define the account manually.")

    # Creating the directory for logs and output
    logs_path, output_path = output_dir(path_to_output=path_to_output,
                                        loglevel=loglevel)

    # Creating the extra arguments for the job submission
    if exclusive:
        extra_args = [
            "--error=" + str(logs_path) + "/dask-worker-%j.err",
            "--output=" + str(output_path) + "/dask-worker-%j.out",
            "--get-user-env",
            "--exclusive"
        ]
    else:
        extra_args = [
            "--error=" + str(logs_path) + "/dask-worker-%j.err",
            "--output=" + str(output_path) + "/dask-worker-%j.out"
        ]

    if max_resources:
        memory, walltime, cores, sockets_per_node, cores_per_socket, threads_per_core = max_resources_per_node(queue=queue)
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
    logger.degug(f"Client: {client}")
    logger.info("Submitting the job to the SLURM queue")
    logger.warning(cluster.job_script())

    # Scaling the cluster
    cluster.scale(jobs=jobs)


def scancel(all=True, Job_ID=None, loglevel='WARNING'):
    """
    scancel() is used to signal or cancel jobs in the queue

    Args:
        all (bool, optional):       If all is True, the function cancels
                                    all user jobs in the queue.
        Job_ID (str, optional):     The SLURM_JOB_ID of a job to cancel in the queue.
                                    Defaults to None.
        loglevel (str, optional):   The level of logging.
                                    Defaults to 'WARNING'.
    """
    logger = log_configure(log_level=loglevel, log_name='slurm')

    if Job_ID is None:
        if all:
            logger.info("Cancelling all user jobs in the queue")
            os.system("scancel -u ${USER}")
    else:
        logger.info("Cancelling the job with ID: " + str(Job_ID))
        os.system("scancel " + str(Job_ID))

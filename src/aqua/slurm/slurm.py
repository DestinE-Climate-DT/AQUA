import os
import re

from dask_jobqueue import SLURMCluster  # pip
from dask.distributed import Client
from aqua.logger import log_configure
from aqua.util import create_folder, load_yaml
from aqua import __path__ as pypath

aqua_dir = os.path.dirname(pypath[0])
# Define the relative path to the 'slurm' folder and the configuration file
slurm_config_file = os.path.join(aqua_dir, 'aqua', 'slurm', 'config-slurm.yml')

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


def get_config(machine_name=None, config_name=slurm_config_file, loglevel='WARNING'):
    """
    Retrieve configuration settings for a specified machine from a YAML file. If the specified
    machine is not found in the configuration, it logs a warning and returns None.

    Args:
        machine_name (str, optional):   The name of the machine for which to retrieve configuration settings.
                                        Defaults to None.
        config_name (str, optional):    The path to the YAML configuration file that contains machine
                                        specific settings. Defaults to 'config-slurm.yml'.
        loglevel (str, optional):       The logging level to be used for the logger that reports issues or
                                        activities of the function. Defaults to 'WARNING'.

    Returns:
        dict or None: A dictionary containing the configuration for the specified machine if found.
                      Returns None if the machine configuration is not found or if the machine name
                      is not specified.
    """
    config = load_yaml(infile=config_name)
    logger = log_configure(log_level=loglevel, log_name='slurm')

    if machine_name:
        machine_config = config.get('machines', {}).get(machine_name)
        if machine_config:
            return machine_config
        else:
            logger.warning(
                f"No specific configuration found for machine '{machine_name}'. Since the machine is unknown, "
                f"there's no default setting available. You will need to manually specify all necessary configurations "
                f"such as queue, account, walltime, memory, cores, jobs, loglevel, and path_to_output."
            )
            return None
    else:
        logger.warning("Machine name not specified. Cannot proceed without a valid machine name.")
        return None


def max_resources_per_node(queue=None, machine_name=None, config_name=slurm_config_file, loglevel='WARNING'):
    """
    Extracting the maximum resources available on the node for the queue

    Args:
        queue (str, optional):          The name of the queue to which maximum resources
                                        available on the node are extracted.
                                        Defaults to None.
        machine_name (str, optional):   The name of the machine for which to retrieve configuration settings.
                                        Defaults to None.
        config_name (str, optional):    The path to the YAML configuration file that contains machine
                                        specific settings. Defaults to 'config-slurm.yml'.
        loglevel (str, optional):       The logging level to be used for the logger that reports issues or
                                        activities of the function. Defaults to 'WARNING'.

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
    logger = log_configure(log_level=loglevel, log_name='slurm')

    config = None
    if machine_name:
        config = get_config(machine_name=machine_name, config_name=config_name)
        if not config:
            logger.warning(f"No valid configuration available for machine '{machine_name}'. Please specify all configurations manually.") # noqa

    if not queue:
        if config and 'queue' in config:
            queue = config.get('queue')
        else:
            raise ValueError("Queue name must be specified if no valid machine configuration is available.")

    max_resources = exctract_sinfo("sinfo  --partition=" + str(queue) + " -lNe")

    if max_resources[2] == queue:
        max_memory = str(float(max_resources[6]) / 1024) + " GB"
        max_sockets, max_cores, max_threads = re.split(r'[:]',
                                                       max_resources[5])
        max_cpus = str(int(max_resources[4]) // int(max_threads))
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


def job(exclusive=False, max_resources=False, cores=None, memory=None,
        queue=None, account=None, walltime=None, jobs=None, path_to_output=None,
        loglevel='WARNING', machine_name=None, config_name=slurm_config_file):
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
                                        Defaults to None.
        memory (str, optional):         The real memory required per node.
                                        Defaults to None.
        queue (str, optional):          The name of the queue to which SLURM
                                        submits the job.
                                        Defaults to None.
        walltime (str, optional):       The duration for which the nodes remain
                                        allocated to you.
                                        Defaults to None.
        jobs (int, optional):           The factor of assignment scaling across
                                        multiple nodes. Defaults to None.
        account (str, optional):        The account to which SLURM charges the
                                        job. Defaults to None.
        path_to_output (str, optional): The path to the directory,
                                        which will contain logs/errors and
                                        output of Slurm Jobs. Defaults is None.
        loglevel (str, optional):       The level of logging.
                                        Defaults to 'WARNING'.
        machine_name (str, optional):   The name of the machine for which to retrieve configuration settings.
                                        Defaults to None.
        config_name (str, optional):    The path to the YAML configuration file that contains machine
                                        specific settings. Defaults to 'config-slurm.yml'.

    """
    logger = log_configure(log_level=loglevel, log_name='slurm')

    if machine_name:
        config = get_config(machine_name=machine_name, config_name=config_name)
        if not config:
            logger.warning(f"No valid configuration available for machine '{machine_name}'. Please specify all configurations manually.") # noqa
    else:
        config = None
        logger.warning("Machine name not specified. Please manually specify all configurations such as queue, cores, memory, etc.") # noqa

    # Apply configurations
    cores = cores if cores is not None else config.get('cores') if config else None
    memory = memory if memory is not None else config.get('memory') if config else None
    queue = queue if queue is not None else config.get('queue') if config else None
    account = account if account is not None else config.get('account') if config else None
    walltime = walltime if walltime is not None else config.get('walltime') if config else None
    jobs = jobs if jobs is not None else config.get('jobs') if config else None
    path_to_output = path_to_output if path_to_output is not None else config.get('path_to_output') if config else None
    loglevel = loglevel if loglevel is not None else config.get('loglevel') if config else 'WARNING'

    # Check if necessary configurations are provided
    if not all([cores, memory, queue, walltime]):
        raise ValueError("Please manually specify the necessary configurations: queue, cores, memory, and walltime, OR provide a valid machine name.") # noqa
    # Log the applied configurations
    logger.debug("Submitting job with the following configurations:")
    logger.debug(f"queue={queue}, cores={cores}, memory={memory}, walltime={walltime}, jobs={jobs}, account={account}, path_to_output={path_to_output}, loglevel={loglevel}") # noqa

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
    logger.debug(f"Client: {client}")
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

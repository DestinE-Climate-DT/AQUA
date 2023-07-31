import re
import subprocess
import time

def waiting_for_slurm_response(number=2):
    return time.sleep(number)

def get_squeue_info():
    """
    The function returns the information about all submitted jobs to the Slurm
    queue by the user.

    Returns:
        (str): the information about all submitted jobs
    """
    return str(subprocess.check_output("squeue --user=$USER",
                                       stderr=subprocess.STDOUT,
                                       shell=True))

def get_last_job_id():
    """
    The function returns the JobID of the last submitted job.

    Returns:
        int or NoneType: the JobID
    """
    if re.findall(r'\d+', get_squeue_info()) == []:
        return None
    else:
        return int(re.findall(r'\d+', get_squeue_info())[0])
    
def Job_ID():
    """
    The function returns the JobID of the last submitted job.

    Returns:
        int: The JobID of last submitted job
    """
    waiting_for_slurm_response(4)
    return get_last_job_id()

def get_job_status():
    """
    The function returns the status of the submitted job by the Slurm user.
    Returns:
        str: the status of the submitted job by the Slurm user: 'R', 'PD',
        'CG' or None.
    """
    job_id = Job_ID()

    if str(job_id) in get_squeue_info():
        job_status_in_slurm = "squeue --job="+str(job_id)
        squeue_info = str(subprocess.check_output(job_status_in_slurm,
                                                  stderr=subprocess.STDOUT,
                                                  shell=True))
        job_status = list(filter(None, re.split(' ', re.split('[)\\n]',
                                                              squeue_info)[1])))[5]
        return job_status
    else:
        return None
    
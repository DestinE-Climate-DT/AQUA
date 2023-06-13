"""Test of aqua.slurm module"""
import pytest
from aqua.slurm import slurm

from os import listdir
from os.path import isfile, join

import re
import subprocess
import time


def waiting_for_slurm_response(number=2):
    return time.sleep(number)


@pytest.fixture
def username():
    return "$USER"


def get_squeue_info(username):
    """
    The function returns the information about all submitted jobs to the Slurm
    queue by the user.

    Returns:
        (str): the information about all submitted jobs
    """
    return str(subprocess.check_output("squeue --user="+str(username),
                                       stderr=subprocess.STDOUT,
                                       shell=True))


def get_last_job_id(username):
    """
    The function returns the JobID of the last submitted job.

    Returns:
        int or NoneType: the JobID
    """
    if re.findall(r'\d+', get_squeue_info(username)) == []:
        return None
    else:
        return int(re.findall(r'\d+', get_squeue_info(username))[0])


@pytest.mark.sbatch
def test_slurm_availability_for_user(username):
    """Testing the slurm availability for the user on the current machine """
    assert "Invalid user" not in get_squeue_info(username)
    assert "command not found" not in get_squeue_info(username)
    assert "error" not in get_squeue_info(username)


@pytest.mark.sbatch
def test_job_submition(username):
    """Testing the submition of the job to Slurm queue """
    old_Job_ID = get_last_job_id(username)
    if old_Job_ID is not None:

        slurm.job()

        waiting_for_slurm_response()
        new_Job_ID = get_last_job_id(username)
        assert new_Job_ID != old_Job_ID
    else:
        slurm.job()
        waiting_for_slurm_response()
        assert get_last_job_id(username) is not None


@pytest.fixture
def Job_ID(username):
    """
    The function returns the JobID of the last submitted job.

    Returns:
        int: The JobID of last submitted job
    """
    waiting_for_slurm_response(4)
    return get_last_job_id(username)


def get_job_status(username, Job_ID):
    """
    The function returns the status of the submitted job by the Slurm user.
    Returns:
        str: the status of the submitted job by the Slurm user: 'R', 'PD',
        'CG' or None.
    """
    if str(Job_ID) in get_squeue_info(username):
        job_status_in_slurm = "squeue --job="+str(Job_ID)
        squeue_info = str(subprocess.check_output(job_status_in_slurm,
                                                  stderr=subprocess.STDOUT,
                                                  shell=True))
        job_status = list(filter(None, re.split(' ', re.split('[)\\n]',
                                                              squeue_info)[1])))[5]
        return job_status
    else:
        return None


@pytest.mark.sbatch
def test_job_presence_in_queue(username, Job_ID):
    """ Testing the presence of the job to Slurm queue """
    assert str(Job_ID) in get_squeue_info(username)


@pytest.mark.sbatch
def test_logfile_creation(username, Job_ID):
    """ Testing the log file creation """
    if get_job_status(username, Job_ID) == 'R':
        waiting_for_slurm_response()
        log_repo = slurm.output_dir()[0]
        log_files = [f for f in listdir(log_repo) if isfile(join(log_repo, f))]
        assert 'dask-worker-'+str(Job_ID)+'.err' in log_files


@pytest.mark.sbatch
def test_outputfile_creation(username, Job_ID):
    """Testing the output file creation """
    if get_job_status(username, Job_ID) == 'R':
        waiting_for_slurm_response()
        output_repo = slurm.output_dir()[1]
        output_files = [f for f in listdir(output_repo) if isfile(join(output_repo, f))]
        assert 'dask-worker-'+str(Job_ID)+'.out' in output_files


@pytest.mark.sbatch
def test_job_cancelation(username, Job_ID):
    """Testing the job cancelation """
    assert str(Job_ID) in get_squeue_info(username)

    if get_job_status == 'CG':
        raise Exception("The job has already been canceled. The current function cannot test the slurm.scalcel(). \
                        Try to do a test with another job")
    else:
        slurm.scancel(all=False, Job_ID=Job_ID)
        waiting_for_slurm_response(4)
        job_status = get_job_status(username, Job_ID)
        assert job_status != 'R' or job_status != 'PD'
        assert job_status == 'CG' or job_status is None

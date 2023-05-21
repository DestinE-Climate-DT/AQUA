"""Test of aqua.slurm module"""
import pytest
from aqua.slurm import slurm

import os 
from os import listdir
from os.path import isfile, join

import re
import subprocess
import time
 
def waiting_for_slurm_response():
    return time.sleep(2)

@pytest.fixture
def username():
    return "$USER"

def get_last_jobid(username):
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    if  re.findall(r'\d+', squeue_info)==[]:
        return None
    else: 
        int(re.findall(r'\d+', squeue_info)[0])

 
    
def test_slurm_availability_for_user(username):
    """Testing the slurm availability for the user on the current machine
    """
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    assert "Invalid user" not  in squeue_info 
    assert "command not found" not in squeue_info
    assert "error" not in squeue_info

def test_job_submition(username):
    """Testing the submition of the job to Slurm queue 
    """
    #slurm.job()
    if get_last_jobid(username)!=None:
        old_Job_ID = get_last_jobid(username)
        slurm.job()
        waiting_for_slurm_response()
        new_Job_ID =  get_last_jobid(username) 
        assert new_Job_ID!=old_Job_ID
    else:
        slurm.job()
        waiting_for_slurm_response()
        if get_last_jobid(username)==None:
            waiting_for_slurm_response()
            waiting_for_slurm_response()
            waiting_for_slurm_response()
        assert get_last_jobid(username)!=None

@pytest.fixture
def Job_ID(username):
    """The function returns the JobID of the last submitted job.
    Returns:
        int: The JobID of last submitted job
    """
    waiting_for_slurm_response()
    waiting_for_slurm_response()
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    return int(re.findall(r'\d+', squeue_info)[0])


@pytest.fixture
def get_job_status(username, Job_ID):
    """The function returns the status of the submitted job by the Slurm user.
    Raises:
        Exception: "The job is not in the Slurm queue"
    Returns:
        str: the status of the submitted job by the Slurm user: 'R', 'P' or 'CG'.
    """
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    if str(Job_ID) in squeue_info:
        job_status_in_slurm="squeue --job="+str(Job_ID)
        squeue_info = str(subprocess.check_output(job_status_in_slurm,  stderr=subprocess.STDOUT, shell=True))
        job_status= list(filter(None, re.split(' ',  re.split('[)\\n]', squeue_info)[1]) ))[5]
        return job_status
    else: 
        raise Exception("The job is not in the Slurm queue")

def test_job_presence_in_queue(username, Job_ID):
    """Testing the presence of the job to Slurm queue 
    """
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    assert str(Job_ID) in squeue_info
    

def test_logfile_creation(Job_ID):
    """ Testing the log file creation
    """
    if get_job_status=='R':
        waiting_for_slurm_response()
        log_repo = slurm.output_dir()[0]
        log_files = [f for f in listdir(log_repo) if isfile(join(log_repo, f))]
        assert 'dask-worker-'+str(Job_ID)+'.err' in log_files

def test_outputfile_creation(Job_ID):
    """Testing the output file creation
    """
    if get_job_status=='R':
        waiting_for_slurm_response()
        output_repo = slurm.output_dir()[1]
        output_files = [f for f in listdir(output_repo) if isfile(join(output_repo, f))]
        assert 'dask-worker-'+str(Job_ID)+'.out' in output_files
    

def test_job_cancelation(username, Job_ID):
    """Testing the job cancelation
    """
    squeue_info = str(subprocess.check_output("squeue --user="+str(username),  stderr=subprocess.STDOUT, shell=True))
    assert str(Job_ID) in squeue_info

    if get_job_status=='CG':
        raise Exception("The job has already been canceled. The current function cannot test the slurm.scalcel(). \
                        Try to do a test with another job")
    else: 
        slurm.scancel(all=False, Job_ID=Job_ID)
        waiting_for_slurm_response()
        waiting_for_slurm_response()
        assert get_job_status!='R' or get_job_status!='P'
        assert get_job_status!='CG' or str(Job_ID) not in squeue_info


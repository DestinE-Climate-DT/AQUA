import os
import time
from aqua import Reader
from aqua.slurm import slurm
from aqua.util import ConfigPath
from aqua.logger import log_configure
from dask.diagnostics import Profiler, ResourceProfiler, visualize

slurm_enable = True
mswep = False

loglevel = 'info'

def waiting_for_slurm_response(number=2):
    return time.sleep(number)

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
    
def generate_catalogue_weights():
    logger = log_configure(log_level=loglevel, log_name='Weights Generator')
    reso="r025"
    zoom=None
    if mswep:
        m, e, s ="MSWEP", "past", "monthly" 
        file_path="/work/bb1153/b382075/aqua/weights/weights_MSWEP_past_monthly_ycon_r025_l2d.nc"
    else:
        m, e, s ="CERES", "ebaf-toa42", "monthly"
        file_path="/work/bb1153/b382075/aqua/weights/weights_CERES_ebaf-toa42_monthly_ycon_r025_l2d.nc"
  
    if os.path.exists(file_path):
        os.remove(file_path)
        logger.info(f"File '{file_path}' removed successfully.")
    else:
        logger.info(f"File '{file_path}' does not exist.")   
    Reader(model=m, exp=e, source=s, regrid=reso, fix=False)

def job_initialization():
    logger = log_configure(log_level=loglevel, log_name='Slurm Initialization')
    # Job initialization 
    if ConfigPath().machine=='levante' or ConfigPath().machine=='lumi':     
        slurm.job(memory="20 GB")
    logger.info('The job is submitted to the queue.')

if __name__ == '__main__': 
    if slurm_enable:
        job_initialization()
    generate_catalogue_weights().compute()
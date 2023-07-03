# Importing the aqua.slurm module and slurm supporting functions nedeed for your script
from aqua.slurm import slurm 
from slurm_supporting_func import get_job_status, waiting_for_slurm_response

####################################################################
#
######## IMPORT HERE MODULES WHICH YOU NEED IN YOUR SCRIPT  ########
#
####################################################################

# Specify the amount of memory, cores which you would like to use during the run!
# By default, slurm.job(cores=1, memory="10 GB", queue = "compute", walltime='02:30:00', jobs=1)
# The default setup can be not enough for your calculations

slurm.job()

# The meaning of the loop is the following: 
# The aqua.slurm module submitted your job to the queue. But you do not want to start your calculations 
# since your job is waiting in the queue and not running yet. Each iteration in the loop checks the status 
# of your job once per minute. If your job was successfully launched to the node and got the running status, 
# your calculations will also start to run. 
 
for i in range(0, 60):
    if get_job_status() == 'R':
        print('The job is started to run!')
        ##############################################################
        #
        ######## PUT THE MAIN PART OF YOUR PYTHON SCRIPT HERE ########
        #
        ##############################################################
        break
    else:
        print('The job is waiting in the queue')
        waiting_for_slurm_response(60)

# Note: The loop will stop to check your job status only for one hour. If the queue is busy, 
# consider increasing the range of your loop.

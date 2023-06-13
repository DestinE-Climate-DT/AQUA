.. _slurm:

Interactive dask-job 
====================

The aqua.slurm module is based on the `dask_jobqueue package <https://jobqueue.dask.org/en/latest/>`_, 
which needs to be installed with pip and it is present as a dependency of AQUA.
The Dask-jobqueue makes it easy to run Dask on job-queuing systems in high-performance supercomputers.
It has a simple interface accessible from interactive systems like Jupyter 
Notebooks or batch Jobs.

The `Slurm` Class
-----------------

The aqua.slurm module contains the `slurm` class, which allows us to create and operate the dask-jobs.
The `slurm` class has the following main functions:

- squeue: allows us to check the status of created Jobs in the queue,
- job: allows the creation and submission of the Job to a selected queue,
- scancel: allows to cancel of all submitted Jobs or only Job with specified Job_ID.


The dask-job initialization 
---------------------------

The job can be launched to the queue with the following command in a Notebook cell:

.. code-block:: python

	slurm.job()
 

The default arguments of slurm.job() function on Levante (`machine=Levante` in configdir) are the followings:

- account = "bb1153",
- queue = "compute",
- cores=1, 
- memory="10 GB",
- path_to_output=".",
- exclusive=False

The default arguments of slurm.job() function, i.e., account and queue names, are different for Lumi (`machine=Lumi` in configdir):

- account = "project_465000454",
- queue = "small"


The function slurm.job() has an argument `exclusive=False` by default. Exclusive argument `exclusive=True` 
is reserving the entire node for the Job.

.. code-block:: python

	slurm.job(exclusive=True)

By default, the user will get the entire `compute` node on Levante and `small` on Lumi. If you would like to reserve a 
node on a different queue, specify the queue's name as an argument of the function:

.. code-block:: python

	slurm.job(exclusive=True, queue = "gpu")


NOTE: The exclusive argument DOES NOT automatically provide us the maximum available memory, number of cores, and walltime.


The function slurm.job() has an argument `max_resources_per_node`, False by default. If we set the argument 
to `max_resources_per_node=True`, the number of cores, memory, and walltime will equal the maximum available
for the choosen node.

.. code-block:: python

	slurm.job(max_resources=True)

If you would like to get the node with maximum resources from the queue, different from the default,  
specify it as an argument of the function:

.. code-block:: python

	slurm.job(max_resources=True,  queue = "gpu")


Path to the output
------------------

The function slurm.job() creates the folders for the job output. By default, the path is ".". 
Therefore, the paths for log and output are: 

- "./slurm/logs" for the errors,
- "./slurm/output/" for the output.

Users can specify the different paths for the SLURM output:

.. code-block:: python

	slurm.job(path_to_output="/any/other/folder/")


The dask-job cancelation
------------------------

The user can cancel all submitted Jobs by

.. code-block:: python
	
	slurm.scancel()

If the user would like to cancel the specific Job,  he needs to know the Job_ID of that Job. 
The Job_ID can be found with the function slurm.squeue(), which returns the information about all user Slurm Jobs on the machine. 
Then the user can cancel the particular Job as:

.. code-block:: python

	slurm.scancel(all=False, Job_ID=5000000)


For more details, please check 
`the slurm Notebook <https://github.com/oloapinivad/AQUA/blob/main/notebooks/slurm/slurm.ipynb>`_.
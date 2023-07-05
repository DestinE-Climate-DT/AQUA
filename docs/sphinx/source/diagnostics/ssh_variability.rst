SSH variability
===============
Description
-----------

This application calculates the sea surface height standard deviation for multiple models (e.g. FESOM, ICON, NEMO) and compares them against the AVISO model. It also provides visualization of the SSH variability for the models. SSH variability provides insights into the complex dynamics of the ocean. It represents the changes in sea surface height over time, which can be influenced by various factors such as ocean currents, wind patterns, tides, and interactions with the atmosphere. By studying SSH variability, we can gain a better understanding of oceanic processes and their impact on climate. High-resolution climate models simulate fine-scale variations in SSH, capturing small-scale features and regional differences highly relevant in the context of climate adaptation for instance, coastal management such as managing coastal hazards like flooding or storm surges.

Structure
-----------

The SSH diagnostic follows a class structure and consists of the files:

* `ssh_class.py`: a python file in which the sshVariability class constructor and the other class methods are included;
* `config.yml`: a yaml file with the required user configurations for the SSH diagnostic;
* `notebooks/ssh_example_4outputs.ipynb`: an ipython notebook which uses the ssh_class and its methods;
* `README.md` : a readme file which contains technical information on how to install the SSH diagnostic and its environment. 

Input variables example
------------------------

* `name` (Model ID e.g. FESOM or ICON)
* `experiment` (Experiment ID)
* `source` (Source ID)
* `regrid` (Perform regridding to grid `regrid`, as defined in `config/regrid.yaml`. Defaults to None.)
* `zoom` (Healpix zoom level)
* `timespan` (time range for which the SSH variability should be calculated)

Output 
------

* SSH variability for each input model in NetCDF format. 
* Visualization of SSH Variability via subplots in PDF format.

Observations
------------

AVISO Sea Surface Height Data

Available demo notebooks
------------------------

Notebooks are stored in diagnostics/SSH/notebooks

* `ssh_example_4outputs.ipynb <https://github.com/oloapinivad/AQUA/blob/devel/ssh2/diagnostics/SSH/notebooks/ssh_example_4outputs.ipynb>`_
        
Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "SSH" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: SSH
    :members:
    :undoc-members:
    :show-inheritance:

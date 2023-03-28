# Automatic load of the shared dependencies between  setup.py  and  enviroment.yml  files. 

##   requirements/conda\_requirements.txt  file


The file contains the shared conda requirements between the ``` enviroment.yml ``` file and ``` setup.py ``` file.

##  requirements/pip\_requirements.txt  file 


The file contains the shared pip requirements between the ``` enviroment.yml ``` file and the ``` setup.py ``` file.


##  Setup.py  file

The function ```  load_requirements ```   imports to to ``` setup.py ``` shared  pip  and  conda  dependencies.

##  Enviroment.yml  file


IMPORTANT! All the information described below is unnecessary because all shared dependencies installed in the new environment by the last line in eviroment.yml:
```
    - -e .
```

But I still describe the way to install shared dependencies to new enviroment:

 - Following command imports shared pip dependencies to yml file
```
- -r file:/work/users/nnazarova/AQUA/requirements/pip_requirements.txt
```
 -  Python script ``` requirements/conda_req_for_yml.py ``` imports shared conda dependencies to file 


##   aqua\_setup\_and\_run.sh  file


Bash script 
 - updates the ``` enviroment.yml ``` file, 
 - deactivates and deletes the previous aqua environment (if aqua is existed and is activated), and 
 - creates and activates(not yet by some reason) a new aqua environment.


## check\_aqua\_env.py  file 

Python script checks if the aqua environment already exists. If an aqua environment already exists
 - and activated, the script deactivates the previous environment
 - script delete the aqua environment from



##  conda\_req\_for\_yml.py  file

Python Script updates the dependencies in the ``` enviroment.yml ``` file in agreement with the ``` conda_requirements.txt ``` file.
If the dependency were already in the ``` enviroment.yml ``` file, the old dependency would be removed, and the new version will be installed


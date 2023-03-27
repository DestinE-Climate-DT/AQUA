# Automatic load of the shared dependencies between  setup.py  and  enviroment.yml  files. 

##   requirements/conda\_requirements.txt  file
File contains the shared conda requirements between ``` enviroment.yml ``` file and ``` setup.py ``` file.

##  requirements/pip\_requirements.txt  file 
File contains the shared  pip  requirements between ``` enviroment.yml ``` file and ``` setup.py ``` file

##  Setup.py  file
``` Setup.py ``` file updated. Now shared  pip  and  conda  dependencies import to ``` setup.py ``` by function ```  load_requirements ```  


##  Enviroment.yml  file
``` Enviroment.yml ``` file updated. Now shared  
 - pip  import to file by command 
```
- -r file:/work/users/nnazarova/AQUA/requirements/pip_requirements.txt
```
 -  conda dependencies import to ``` enviroment.yml ``` by python script ``` requirements/conda_req_for_yml.py ```


##   aqua\_setup\_and\_run.sh  file

File ``` aqua_setup_and_run.sh ``` updates the ``` enviroment.yml ``` file, creates and activates new aqua enviroment. 
Bash file also can delete previus enviroment with the same name (not implemented yet)

##  conda\_req\_for\_yml.py  file
Python Script update the dependencies in ``` enviroment.yml ``` file in agreement with ``` conda_requirements.txt ``` file. 
If dependency was already in the ``` enviroment.yml ``` file, the dependency will be removed and new version will be installed  



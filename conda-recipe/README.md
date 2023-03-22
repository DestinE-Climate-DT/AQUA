# Building and installing the aqua package with conda 

## How to build the aqua package with conda?

As the first step, we need to install the ``` conda-build ```  package:
``` 
conda install conda-build 
```
Also,  it is useful to install ``` conda-verify ```:
```
conda install conda-verify
```
To avoid problems with dependencies, we need to modify/add the following lines in the setup.py file:
 - 'numpy<1.24',
 - 'urllib3<1.27',


Then we need to create the 'conda-recipe' directory. The following files in the 'conda-recipe' are required:
 - ``` bld.bat ```: --Batch file for Windows  
 - ``` build.sh ```: ---Shell script for macOS and Linux 
 - ``` meta.yaml ```

We are building the aqua package with the following command:
``` 
conda build conda-recipe
```

```
conda build purge 
``` 

## How to install the aqua package with conda?

I am installing the aqua package with the command:
```
conda install --use-local aqua
```

Also, we can install the package by specifying the full PATH and its name. You can d the PATH and name after building the package. In my case, it is the following:
```
/home/b/b382267/mambaforge/conda-bld/linux-64/aqua-0.0.2-0.tar.bz2
```
## Hot to update the aqua package?

Give it a new version number in ``` meta.yaml ``` and ``` setup.py ``` and build the package with ``` conda build conda-recipe ```. Updating is simply
```
conda update aqua
```


## How to create the aqua environment base on the built package?
```
conda create -n aqua_env --use-local aqua  # will pull in deps
```

```
source activate myenv  
```

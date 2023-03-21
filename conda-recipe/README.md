# Building and installating the aqua package with conda 

## How to build the aqua package with conda?

As the first step, we need to install the following package:
- conda-build (``` conda install conda-build ```)


In order to avoid problems with dpendences, we need to modify/add the following lines in setup.py file:
 - 'numpy<1.24',
 - 'urllib3<1.27',


Then we need to create the 'conda-recipe' directory. The following files in the 'conda-recipe' required:
 - bld.bat  
 - build.sh  
 - meta.yaml

We are building the aqua package with the following command:
``` conda build conda-recipe
```
```
conda build purge 
``` 

## How to install the aqua package with conda?

After bulding the aqua package we get the PATH to builded aqua package.
In my case, the PATH is '/home/b/b382267/mambaforge/conda-bld/linux-64'.
I am installing the aqua package with command:
```
conda install /home/b/b382267/mambaforge/conda-bld/linux-64/aqua-0.0.2-py310_0.tar.bz2
```

Of course, there should be better way to install the aqua paskage and I will find it.
  

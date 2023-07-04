# Diagnostic of tropical rainfalls

The purpose of this diagnostic is to provide a set of basic statistics and plots for different climate models. These models will be compared to reanalysis data (e.g. ERA5). It uses the AQUA framework to read and retrieve the cataloged data via the 'Reader' class.

## Python Module 

The required module for this diagnostic is `atm_global_mean.py`. 


## Notebooks 

The notebook folder contains the following notebooks:
 - `atm_global_bias_ng2.ipynb` and `agm_using_class_ng2.ipynb`: 

    The notebook demonstrates the major abilities of this diagnostic using the data from the nextGEMS cycle 2: 
    - initialization of an object of the diagnostic class, 
    - selection of the class attributes,  
    - calculation of the histograms in the form of xarray, 
    - saving the graphics in the storage,
 - `agm_ng3.ipynb`:

    The notebook demonstrates the major abilities of this diagnostic using the data from the nextGEMS cycle 3

##  Enviroment 




# Atmospheric global mean biases diagnostic 

Main authors: 
- Susan Sayed (DWD, susan.sayed@dwd.de)

## Description

The purpose of this diagnostic is to provide a set of basic statistics and plots for different variables and climate models. These models will be compared to reanalysis data (e.g. ERA5) or with other models or climatologies. It uses the AQUA framework to read and retrieve the cataloged data via the 'Reader' class. The diagnostic reads various experiments through the Low Resolution Archive (LRA) to compute and plot global mean biases.

## Python Module 

The required module for this diagnostic is `atmglobalmean` from the atm_global_mean.py file. 
Some variables that can be analyzed within this diagnostic are:
- 2m temperature, 
- precipitation, 
- radiative fluxes
- zonal and meridional wind, 
- temperature, 
- specific humidity


## Notebooks 

The notebook folder contains the following notebooks:
 - `agm_ng3_seasons.ipynb`: 

    The notebook demonstrates the major abilities of this diagnostic using the data from the nextGEMS cycle 3: 
    - initialization of an object of the diagnostic class, 
    - selection of the class attributes,  
    - plotting of seasonal biases:
        a) either between two different models
        b) or between a model and ERA5 climatology (2000-2020)
- `agm_ng3_plev.ipynb`: 

    The notebook demonstrates the major abilities of this diagnostic using the data from the nextGEMS cycle 3: 
    - plotting biases along the vertical axis for a certain time range for the model. The data will be compared with the ERA5 climatology or another dataset
    
- `agm_ng3_mean_plots.ipynb`: 

    The notebook provides a way to generate some very basic plots and statistics to examine to single models from the nextGEMS cycle 3

The output of this diagnostic are plots and NetCDF data files that are stored in the output folder.



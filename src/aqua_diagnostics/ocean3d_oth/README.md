# Ocean3D diagnostic

Main authors: 
- Supriyo Ghosh (BSC, supriyo.ghosh@bsc.es)
- Pablo Ortega (BSC, pablo.ortega@bsc.es)

## Description

The current release of Ocean3D diagnostics includes two submodules with dedicated functions and notebooks: `ocean_drifts`  to characterise and monitor model drifts and `ocean_circulation` to evaluate the realism of the model in simulating key precursors of the ocean circulation

## Table of Contents

- [Diagnostic of Ocean3D](#diagnostic-of-Ocean3D)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

The diagnostic uses a standard aqua environment and requires no extra dependencies. 


To install of the diagnostic on Levante or Lumi, please follow the installation instructions in the main [README.md](https://github.com/oloapinivad/AQUA/blob/main/README.md) file of aqua.

## Data requirements  

The diagnostic requres a model data of 3D ocean potential temperature and practical salinity.

## Examples
The **notebook/** folder contains the notebook 

**[ocean_drifts.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/ocean3d/notebooks/ocean_drifts.ipynb)**, which explains:
- how to import the `ocean_drifts`module and load it in its standard configuration, 
- how to use the main diagnostic functions of the `ocean_drifts` module, which produce hovmollers in-depth and time, time-series of yearly potential ocean temperature and practical salinity at different depths, maps of temporal trends in latitude-longitude and latitude-depth space.
- the input data requierements, 
- how to interpret and where to store the main scientific outputs obtained with the different functions.

 **[ocean_circulation.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/ocean3d/notebooks/ocean_circulation.ipynb)**, which explains:
- how the import of `ocean_circulation`module and load it in its standard configuration, 
- how touse the main diagnostic functions of the `ocean_circulation` module, which compute and plot the climatologies of the mixed layer depth and density stratification profiles in different regions of the world,
- the input data requierements, 
- the application of primary functions, and 
- how to interpret and where to store the main scientific outputs obtained with the different functions.

## Contributing

The  ocean3d module is in a developing stage and will be significantly improved in the near future. If you have any suggestions, comments, or problems with its usage, please get in touch with the AQUA team, particulaty with Supriyo Ghosh (supriyo.ghosh@bsc.es) or Pablo Ortega (pablo.ortega@bsc.es).

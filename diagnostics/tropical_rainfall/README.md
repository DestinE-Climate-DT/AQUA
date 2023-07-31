# Diagnostic of tropical rainfalls

Main authors: 
- Natalia Nazarova (POLITO, natalia.nazarova@polito.it)

## Description

The tropical-rainfall diagnostic analyzes rainfall variability in the tropical zone and compares the climatological models' predictions with observations.  

The module comprises Python-implemented source files, an environment file, tests, demonstration files, and a command line interface. You can find a detailed description of the module in the aqua documentation. 

Below you can find a quick start to the tropical_rainfall diagnostic.

## Table of Contents

- [Diagnostic of tropical rainfalls](#diagnostic-of-tropical-rainfalls)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
    - [Installation on Levante](#installation-on-levante)
    - [Installation on Lumi](#installation-on-lumi)
  - [Data requirements](#data-requirements)
  - [Output](#output)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

### Installation on Levante


`env-tropical-rainfall.yml` file contains the minimal requirement for tropical diagnostic. 


You can create the environment with the use of conda

```
conda env create -f diagnostics/tropical_rainfall/env-tropical-rainfall.yml
```

or with mamba
```
conda env create -f diagnostics/tropical_rainfall/env-tropical-rainfall.yml
```

### Installation on Lumi


The easiest way to install the diagnostic is by using the pip installation method and adding the following packages
- fast_histogram
- boost_histogram
- dask_histogram

to the list of pip packages in the aqua installation script, which is located in the folder `/config/machines/lumi/installation/`.

## Data requirements  

The following are the requirements for input climatological data:
Data must include the precipitation rate variable (**tprate**) in a latitude and longitude grid. 


## Output 

All output of the diagnostic is in the format of NetCDF or PDF.
Folder where to store the output can be specified by the user.

## Examples

The **notebook/** folder contains the following notebooks:
 - **([histogram_calculation.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/histogram_calculation.ipynb))**:

    The notebook demonstrates the major abilities of tropical rainfall diagnostic: 
    - initialization of an object of the diagnostic class, 
    - selection of the class attributes,  
    - calculation of the histograms in the form of xarray, 
    - saving the histograms in the storage,
    - and loading the histograms from storage.
 - **([histogram_plotting.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/histogram_plotting.ipynb))**:

    The notebook demonstrates the abilities of the histogram plotting functions:
    - selection of the plot style: step line style, 2D smooth line style, and different color maps,
    - selection of the plot size, axes scales, 
    - saving plot into storage, 
    - plotting the counts, frequencies, and Probability density function (pdf) from the obtained histograms.
 - **([diagnostic_vs_streaming.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/diagnostic_vs_streaming.ipynb))**:

    The notebook demonstrates the usage of diagnostic during the streaming mode:
    - saving the obtained histogram with the histogram into storage per each chunk of any data during the stream, 
    - loading all or multiple histograms from storage and merging them into a single histogram. 

 - **([comparison_of_lowres_models.ipynb](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/comparison_of_lowres_models.ipynb))**:

    The notebook demonstrates:
    - histogram comparison for different climate models,
    - the ability to merge a few separate plots into a single one, 
    - mean of tropical and global precipitation calculations for different climate models,
    - bias between climatological model and observations. 


## Contributing

The tropical_rainfall module is in a developing stage and will be significantly improved in the near future. If you have any suggestions, comments, or problems with its usage, please get in touch with the AQUA team or Natalia Nazarova (natalia.nazarova@polito.it).

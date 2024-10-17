# Diagnostic of tropical rainfalls

Main authors: 
- Natalia Nazarova (POLITO, natalia.nazarova@polito.it)

## Description

The Tropical-Rainfall Diagnostic analyzes rainfall variability in the tropical zone and compares predictions from climatological models with observations.

The module comprises Python-implemented source files, an environment configuration file, tests, demonstration files, and a command-line interface. A detailed description of the module is available in the AQUA documentation.

Below is a quick start guide for the Tropical Rainfall Diagnostic.

## Table of Contents

- [Diagnostic of tropical rainfalls](#diagnostic-of-tropical-rainfalls)
  - [Description](#description)
  - [Table of Contents](#table-of-contents)
  - [Installation Instructions](#installation-instructions)
  - [Data requirements](#data-requirements)
  - [Output](#output)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation Instructions

The simplest method to install the Tropical Rainfall Diagnostic is through pip. Add the package to your list of pip dependencies:
```
pip install $AQUA/diagnostics/tropical_rainfall/
```
Alternatively, you can include the package in your Conda environment by adding the following line to your Conda environment file:
```
-e $AQUA/diagnostics/tropical_rainfall/
```
The installation requires **fast_histogram** among other dependencies. The installation of the Tropical Rainfall package is integrated into the AQUA package installation process.

For detailed installation instructions specific to environments like Levante or Lumi, please refer to the **Installation** sections in the 
**[README.md](https://github.com/DestinE-Climate-DT/AQUA/blob/main/README.md)** file of the AQUA package.


## Data requirements  

Input data must include the precipitation rate variable (**mtpr**) on a latitude and longitude grid.

The diagnostic can be performed on data of any spatial and temporal resolution.

## Output 

All diagnostic outputs are provided in either NetCDF or PDF formats. Users can specify the output storage directory in the config file located at `$AQUA/diagnostics/tropical_rainfall/tropical_rainfall/config-tropical-rainfall.yml`, or directly during the initialization of the diagnostic:
```
diag = Tropical_Rainfall(path_to_netcdf = 'your/path/to/netcdf/', path_to_pdf = 'your/path/to/pdf/')
```


## Examples

The **notebooks/** folder contains several notebooks demonstrating various functionalities::
 - **[Demo for Low-Resolution Data](https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/demo_for_lowres_data.ipynb)**:
   - Histogram comparison for different climate models
   - Merging separate plots into a single one
   - Mean tropical and global precipitation calculations for different climate models
   - Bias between climatological model and observations
 - **[Histogram Calculation](https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/functions_demo/histogram_calculation.ipynb)**:
   - Initialization of a diagnostic class object
   - Selection of class attributes
   - Calculation of histograms in the form of xarray
   - Saving histograms in storage
   - Loading histograms from storage
 - **[Histogram Plotting](https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/functions_demo/histogram_plotting.ipynb)**:
   - Selection of plot styles and color maps
   - Adjusting plot size and axes scales
   - Saving plots into storage
   - Plotting counts, frequencies, and probability density functions from histograms
 - **[Diagnostic vs. Streaming](https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/functions_demo/diagnostic_vs_streaming.ipynb)**:
   - Saving histograms per data chunk during streaming
   - Loading and merging multiple histograms into a single histogram
 - **[Data Attributes](https://github.com/DestinE-Climate-DT/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/functions_demo/data_attributes.ipynb)**:
   - Saving high-resolution data chunks with unique filenames including `time_band`
   - Automatically updating `time_band` when merging datasets
   - Ensuring merged datasets reflect the accurate total time band

## Contributing

The tropical_rainfall module is in the development stage and will be significantly improved in the near future. If you have suggestions, comments, or issues with its usage, please contact the AQUA team or Natalia Nazarova (natalia.nazarova@polito.it).

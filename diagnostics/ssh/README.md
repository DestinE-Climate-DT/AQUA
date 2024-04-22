# Sea Surface Height (SSH) Variability Diagnostics Application

Main authors:
- Tanvi Sharma (AWI, tanvi.sharma@awi.de)
Developments and modifications:
- Jaleena Sunny (AWI, jaleena.sunny@awi.de)

## Description

This application calculates the sea surface height standard deviation for multiple models (e.g. FESOM, ICON, NEMO) and compares them against the AVISO model. It also provides visualization of the SSH variability for the models.

## Requirements (majority of them are covered under environment set up for AQUA)
- Cartopy

## Installation
The code works on both py3.10 and py3.9.  Recommended installation through mamba (a package manager for conda-forge). Please follow the README.md instructions for AQUA.

## Configuration
The application requires a YAML configuration file (`config.yaml`) to specify the settings.

## Usage
1. Configure the config.yaml file with the desired settings.
2. Run the application
The application will calculate the SSH standard deviation for AVISO and the other specified models, save the results as NetCDF files, generate subplots for visualization, and save the subplots as a JPEG image.

## Output
The code produce both NetCDF files for storing output and figures. 
- NetCDF files: The computed SSH standard deviation for each model is saved as separate files. 
Output are stored on the original model grid. For unstructured grids, this data has been regridded using AQUA regridder for visualization only.
- Figures: the subplots showing the SSH variability for each model are as a PNG.

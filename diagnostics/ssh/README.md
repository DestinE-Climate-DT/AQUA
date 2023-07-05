# Sea Surface Height (SSH) Variability Diagnostics Application

This application calculates the sea surface height standard deviation for multiple models (e.g. FESOM, ICON, NEMO) and compares them against the AVISO model. It also provides visualization of the SSH variability for the models.

## Requirements (majority of them are covered under environment set up for AQUA)
- Cartopy

## Installation
The code works on both py3.10 and py3.9.  Recommended installation through mamba (a package manager for conda-forge). Please follow the README.md instructions for AQUA.

## Configuration

The application requires a YAML configuration file (`config.yml`) to specify the settings which can be found in config.yml file.

## Usage
1. Configure the config.yml file with the desired settings.
2. Run the application
The application will calculate the SSH standard deviation for AVISO and the other specified models, save the results as NetCDF files, generate subplots for visualization, and save the subplots as a JPEG image.

## Output
NetCDF files: The computed SSH standard deviation for each model will be saved as separate NetCDF files. These output files are ungridded data. For visualization this data is regridded using AQUA regridder.
Subplots: The subplots showing the SSH variability for each model will be saved as a JPEG image.
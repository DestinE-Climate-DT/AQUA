# Sea Surface Height (SSH) Variability Diagnostics Application

Author:
- Maqsood Mubarak Rajput (AWI, maqsoodmubarak.rajput@awi.de) (Author and maintainer v0.19.1)
- Tanvi Sharma (AWI, tanvi.sharma@awi.de) (Author older version)
- Jaleena Sunny (AWI, jaleena.sunny@awi.de) (Contributor older version)

## Description

This application calculates the sea surface height standard deviation for models namely, FESOM, ICON and NEMO. It compares them against the AVISO model. It also provides visualization of the SSH variability for the models.

## Installation Instructions

To install this diagnostic you can use conda.

No more environments than the regular AQUA ones (located in `./environment.yaml`) are needed.
Refer to the AQUA documentation for more information.

## Configuration
The application requires a YAML configuration file which are available `AQUA/config/diagnostics/sshVariability` to specify the settings.

## Usage
1. Configure the `config_ssh.yaml` file with the desired settings.
2. Run the application via CLI or the notebook available in `notebooks/diagnostics`.
The application will calculate the SSH standard deviation for AVISO and the other specified models, save the results as NetCDF files, generate plots for visualization, and save the subplots as a PNG or PDF image.

## Output
The code produce both NetCDF files for storing output and figures. 
- NetCDF files: The computed SSH standard deviation for each model is saved as separate NetCDF files. 
Output are stored on the model's input grid.
- Figures: the plots and subplots showing the SSH variability for each model are as PNG or PDF.
- Additionally, difference plots can be created. 

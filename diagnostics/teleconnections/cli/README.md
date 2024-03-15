# Teleconnections CLI

This folder contains the code to perform the analysis with the teleconnections diagnostic.

## Usage

The CLI script is called `cli_teleconnections.py` and can be used as follows:

```bash
mamba activate aqua # if you are using the conda environment
python cli_teleconnections.py
```

### Configuration

Additional options are:
-  `-c` or `--config`: path to the configuration file
-  `-d` or `--dry`: if True, run is dry, no files are written
-  `-l` or `--loglevel`: log level [default: WARNING]
-  `--ref`: if True, analysis is performed against a reference
-  `--model`: model name
-  `--exp`: experiment name
-  `--source`: source name
-  `--outputdir`: output directory
-  `--interface`: interface file

Configuration files can be found in this folder and are named `cli_config_*.yaml`:
- `cli_config_atm.yaml` is an example configuration file for the atmosphere part of the teleconnections diagnostic. It is the default configuration file.
- `cli_config_oce.yaml` is an example configuration file for the ocean part of the teleconnections diagnostic.

The configuration file is a YAML file with the following structure:

```yaml
# you can turn on/off the atmosferic teleconnections you want to compute
teleconnections:
  NAO: true

# The first is overwritten if the script with the flags --model, --exp, --source
# Extra keyword arguments for the models are:
# regrid: null
# freq: null
# zoom: null
# These are all null (None) by default because we're assuming
# data are monthly and already regridded to a 1deg grid.
# If you want to use native data, you have to set these parameters
# for each model.
models:
  - model: 'IFS'
    exp: 'tco2559-ng5-cycle3'
    source: 'lra-r100-monthly'

# Reference is analyzed if --ref is passed to the script
# The same extra keyword arguments as for the models can be used.
reference:
  - model: 'ERA5'
    exp: 'era5'
    source: 'monthly'

# This is overwritten if the script with the flags --configdir
# Common output directory for all teleconnections
# Structure of the output directory:
# outputdir
# ├── pdf
# └── netcdf
outputdir: './output' # common output directory for figures and netcdf files

# Configdir for the teleconnections
configdir: null

# List of teleconnections specific configuration parameters
NAO:
  months_window: 3
```

## Computational requirements

The diagnostic require to have the correct environment loaded.
It requires in order to produce all the figures if `--ref` is set the ERA5 reanalysis data in the catalogue, as `model=ERA5`, `exp=era5`, `source=monthly`.
We provide a slurm script to run the diagnostic but the execution on a low resolution archive is very fast and an interactive session is sufficient.

Please note that a wrapper to execute all the diagnostics is provided in the `cli/aqua-analysis` folder.
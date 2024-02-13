# Tropical Cyclones CLI

This folder contains the commandi line interface (CLI) to run the tropical cyclones detection, tracking and zoom in 
diagnostic on a selected experiment.

## Usage

The CLI script is called `tropical_cyclones_cli.py` and can be used as follows:

```bash
mamba activate aqua
tropical_cyclones_cli.py --config <path_to_config_file>
```
In case the config file is in the same directory of the CLI there is no need to specify the path.
An example of a configuration files can be found in this folder, please refer to the notes in the configuration files for more information on the options.

A more detailed description of additional options can be found by running `python tropical_cyclones.py --help`.
If a configuration is specified both in the configuration file and as a command line argument, the command line argument takes precedence.

## Execution with slurm

The tropical cyclones CLI can be used in combination with slurm to submit a job in LEVANTE or LUMI. Please refer to the 
`run_TCs_LEVANTE.job` and `run_TCs_LUMI.job` to set the SBATCH options and adjust the output folder path according to the machine you are using. To launch a slurm job it is sufficient to type:
```
slurm run_TCs_MACHINE.job
```

## CLI output
The execution of the tropical cyclones diagnostic does not produce any plot, but only the output of the tropical cyclones detection and tracking of the full resolution data in the vicinity of the detected TCs as NetCDF files. The variables to be stored by the zoom in diagnostic can be set in the `config_tcs.yaml` file.
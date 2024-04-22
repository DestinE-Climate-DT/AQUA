This folder contains the CLI for the Global Time Series diagnostic.

## Available CLI

The CLI is able to run timeseries, seasonal cycles and Gregory-like plots.
Separate configuration files are given for atmosphere and ocean timeseries and seasonal cycles.

By default the models are compared against ERA5.

## CLI options

The CLI has the following options:

- `--config`, `-c`: Path to the configuration file.
- `--nworkers`, `-n`: Number of workers to use for parallel processing.
- `--loglevel`, `-l`: Logging level. Default is `WARNING`.
- `--model`: Model to analyse. It can be defined in the config file.
- `--exp`: Experiment to analyse. It can be defined in the config file.
- `--source`: Source to analyse. It can be defined in the config file.
- `--outputdir`: Output directory for the plots.

## Usage

To run the CLI, you can use the following command:

```bash
python diagnostics.global_time_series.cli --config <path_to_config_file> --nworkers <number_of_workers> --loglevel <logging_level> --model <model> --exp <experiment> --source <source> --outputdir <output_directory>
```

A submitter script for LUMI is available in this folder.
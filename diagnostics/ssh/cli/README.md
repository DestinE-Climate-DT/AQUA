# CLI for SSH variability diagnostic

This CLI tool provides the ability to run the SSH (Sea Surface Height) variability diagnostic. It primarily uses a configuration file to obtain parameters for the diagnostic, but command-line arguments can also be provided to override specific values from the configuration file.

## Usage:

The diagnostic is initiated based on the configurations defined in the provided YAML file. Currently, only one model comparison to the AVISO data is supported via the CLI.

Configuration for the diagnostic is first read from the configuration file, specified using the `--config` argument. Subsequently, any values in the configuration can be updated using command line arguments.

### Basic command

```bash
python ./ssh_cli.py --config=./config.yaml
```

### Command with Override Options:

If you wish to override specific configurations from the configuration file, you can use the following command line arguments:

```bash
python ./ssh_cli.py --config=./config.yaml --model=$MODEL --exp=$EXP --source=$SOURCE --outputdir=$OUTPUTDIR/dummy
```

Command Line Arguments:

* `--config`: Specifies the path to the YAML configuration file.
  - Default: (The default path is provided in the script. Usually something like `./config.yaml`)  
* `--model`: Overrides the model name in the configuration file.
* `--exp`: Overrides the experiment name in the configuration file.
* `--source`: Overrides the source name in the configuration file.
* `--outputdir`: Overrides the output directory in the configuration file.
* `--modeltime`: Sets the model time span. This takes two dates in the format "YYYY-MM-DD", "YYYY-MM-DD". 
  - Example: `--modeltime "2022-01-01" "2022-12-31"`
* `--obstime`: Sets the observation time span. This takes two dates in the format "YYYY-MM-DD", "YYYY-MM-DD".
  - Example: `--obstime "2022-01-01" "2022-12-31"`
* `--nworkers`: Sets the number of workers, dask
  - Example: `--nworkers 4`
* `--regrid`: Sets the required resolution
  - Example: `--regrid r010`


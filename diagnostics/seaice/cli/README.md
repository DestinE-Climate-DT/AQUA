# CLI for sea ice diagnostics

This CLI tool provides the ability to run the sea ice diagnostics. It primarily uses a configuration file to obtain parameters for the diagnostic, but command-line arguments can also be provided to override specific values from the configuration file.

## Usage:

The diagnostic is initiated based on the configurations defined in the provided YAML file. Currently, only one model comparison to the OSISAF data is supported via the CLI.

Configuration for the diagnostic is first read from the configuration file, specified using the `--config` argument. Subsequently, any values in the configuration can be updated using command line arguments.

### Basic command

```bash
python ./seaice_cli.py --config=./config.yml
```

### Command with Override Options:

If you wish to override specific configurations from the configuration file, you can use the following command line arguments:

```bash
python ./seaice_cli.py --config=./config.yml --model=$MODEL --exp=$EXP --source=$SOURCE --outputdir=$OUTPUTDIR/dummy
```

Command Line Arguments:

* `--config`: Specifies the path to the YAML configuration file.
* `--model`: Overrides the model name in the configuration file.
* `--exp`: Overrides the experiment name in the configuration file.
* `--source`: Overrides the source name in the configuration file.
* `--outputdir`: Overrides the output directory in the configuration file.
* `--regrid`: Overrides the regrid option in the configuration file.



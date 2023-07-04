# Teleconnections diagnostic

## Table of contents

- [Teleconnections diagnostic](#teleconnections-diagnostic)
  - [Table of contents](#table-of-contents)
  - [Description](#description)
    - [Teleconnections available:](#teleconnections-available)
  - [Python module](#python-module)
  - [Installation instructions](#installation-instructions)
    - [Installation on Levante](#installation-on-levante)
    - [Add kernel for DKRZ jupyterhub](#add-kernel-for-dkrz-jupyterhub)
    - [Installation on Lumi](#installation-on-lumi)
  - [Data requirements](#data-requirements)
  - [Library files](#library-files)
  - [Examples](#examples)
  - [Tests](#tests)
  - [Contributing](#contributing)

## Description

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `AQUA` framework.

### Teleconnections available:

At the moment the following teleconnections are available:

- NAO (see NAO notebooks)
- ENSO (see ENSO notebooks)

These two teleconnections are evaluated with station-based indices or with the usage of regional means.
Other teleconnections can be configured by modifying the `teleconnections.yaml` file in the `config` folder.

See the documentation for more details on the teleconnections.

## Python module

The python package is based on the `AQUA` framework.
A list of its dependencies can be found in the `environment.yml` file in the `AQUA` root of the repository.
Additionally, a specific list of the packages needed other that the framework environment can be found in the `env-teleconnections.yml` file, that can be used as well to create the `conda` environment needed to run the diagnostic.
The extra packages are:

- python-cdo (needed for tests only)

Teleconnections evaluation without tests has the same dependencies of the `AQUA` framework.

The `teleconnections` environment installs the `AQUA` framework with the `pip` installation method.
For this reason the `teleconnections` environment can have slightly different versions of the packages than the `AQUA` framework.

The `teleconnections` environment is compatible with python 3.9 and 3.10 as the `AQUA` framework.


## Installation instructions

The `env-teleconnections.yml` file can be used to create the `conda` environment needed to run the diagnostic.
It is also possible to install the `teleconnections` environment with `pip`, since a `pyproject.toml` file is provided.

### Installation on Levante

To install the diagnostic on `Levante` you can use `mamba` or `conda`.
```bash
mamba env create -f env-teleconnections.yaml # or conda
```

To install the diagnostic in an existing conda environment run:

```bash
conda env update -f env-teleconnections.yaml
```

To activate the environment run:

```bash
conda activate teleconnections
```

or the name of the environment you chose to update.
Please be aware that the command line tools require the existance of the `teleconnections` environment, change the submitter script accordingly.

The diagnostic environment is compatible with python 3.9 and 3.10 and with the AQUA framework.
Different diagnostic environments may be not compatible with each other.
If you want to use multiple diagnostics at the same time, it is recommended to use the different environments for each of them.

### Add kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

After creating the environment, activate it and install the kernel:

'''bash
python -m ipykernel install --user --name teleconnections --display-name="teleconnections"
'''

### Installation on Lumi

Since `conda` is not available on Lumi, a different installation procedure is provided.
The `lumi_installation.sh` script can be used to install the diagnostic on Lumi.

Since at the moment the `AQUA` framework is all is needed to run the diagnostic, the `lumi_installation.sh` script available to install the `AQUA` framework is enough to run the diagnostic.

It is available in the `AQUA` repository, in the `config/machines/lumi/installation` folder.

## Data requirements

The diagnostic requires the following data:
- 'msl' (mean sea level pressure) for NAO
- 'sst' (sea surface temperature) for ENSO

Data should be preferably in the form of monthly means and it would be optimal for efficiency to have data on a grid with a resolution of 1°x1°.
This can be achieved by using the `regrid` option in the `AQUA` framework, with `regrid='r100'`, default value in the class initialization.

Comparisons with observations are also available, and require to have access to ERA5 data.
Data are already available on Levante and Lumi.

Additionally, NCAR data with monthly values of NAO and ENSO indices are available in the `data` folder.

## Library files

- `cdotesting.py` contains function evaluating teleconnections with cdo bindings, in order to test the python libraries (see tests section).
- `index.py` contains functions for the direct evaluation of teleconnection indices. It is the core of the diagnostic.
- `plots.py` contains functions for the visualization of time series and maps for teleconnection diagnostic.
- `tc_class.py` contains the class that is used to run the diagnostic.
- `tools.py` contains generic functions that may be useful to the whole diagnostic.

## Examples

All notebooks are in the `notebooks` folder.

- `NAO/ENSO` contain the respective teleconnections evaluated with the library methods from `index.py` and `plots.py`.
- The same analysis performed on the nextGEMS cycle3 data is available in thesame folder.

## Tests

Tests are run with github actions, see `.github/workflows/teleconnections.yml` for details.
Tests make use of the `cdotesting.py` file, that contains functions evaluating teleconnections with cdo bindings.

## Contributing

Contributions are welcome, please open an issue or a pull request.
If you have any doubt or suggestion, please contact the AQUA team or Matteo Nurisso (m.nurisso@isac.cnr.it).
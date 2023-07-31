# Teleconnections diagnostic

Main authors: 
- Matteo Nurisso (m.nurisso@isac.cnr.it)

## Description

The folder contains jupyter-notebooks and python scripts in order to evaluate teleconnections in the DE_340 AQUA model evaluation framework.
The script are based on the `AQUA` framework.

At the moment the following teleconnections are available:
- [NAO](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb)
- [ENSO](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb)


See the documentation for more details on the teleconnections.

## Table of contents

- [Teleconnections diagnostic](#teleconnections-diagnostic)
  - [Description](#description)
  - [Table of contents](#table-of-contents)
  - [Installation instructions](#installation-instructions)
    - [Installation on Levante](#installation-on-levante)
      - [Add kernel for DKRZ jupyterhub](#add-kernel-for-dkrz-jupyterhub)
    - [Installation on Lumi](#installation-on-lumi)
  - [Data requirements](#data-requirements)
  - [Examples](#examples)
  - [Contributing](#contributing)

## Installation instructions

The `env-teleconnections.yml` file can be used to create the `conda` environment needed to run the diagnostic.
It is also possible to install the `teleconnections` environment with `pip`, since a `pyproject.toml` file is provided.
The actual version of the `env-teleconnections.yml` file installs both the `AQUA` framework and the `teleconnections` packages with `pip`.

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
Please be aware that the command line tools require the existance of the `teleconnections` environment, change the submitter script accordingly if you want to use a different environment name.

The diagnostic environment is compatible with python 3.9 and 3.10.
Different diagnostic environments may be not compatible with each other, so if you're using multiple diagnostics, it is recommended to a different environment for each of them.

#### Add kernel for DKRZ jupyterhub

Documentation on adding kernels: https://docs.dkrz.de/doc/software%26services/jupyterhub/kernels.html#use-your-own-kernel

After creating the environment, activate it and install the kernel:

'''bash
python -m ipykernel install --user --name teleconnections --display-name="teleconnections"
'''

### Installation on Lumi

Since `conda` is not available on Lumi, a different installation procedure is required.
The simplest way to install the diagnostic is to use the `pip` installation method, adding the package to the list of pip packages in the aqua installation script, available in the `/config/machines/lumi/installation/` folder.

## Data requirements

The diagnostic requires the following data:
- 'msl' (mean sea level pressure) for NAO
- 'sst' (sea surface temperature) for ENSO

Data should be preferably in the form of monthly means and it would be optimal for efficiency to have data on a grid with a resolution of 1°x1°.
This can be achieved by using the `regrid` option in the `AQUA` framework, with `regrid='r100'`, default value in the class initialization.
It is possible to initialize the class with different regridding and time aggregation options, so that the diagnostic can deal with different resolutions and time frequencies.

Comparisons with observations are also available, and require to have access to ERA5 data.
Data are already available on Levante.

Additionally, NCAR data with monthly values of NAO and ENSO indices are available in the `data` folder.

## Examples

All notebooks are in the `notebooks` folder.

- [NAO](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO.ipynb)/[ENSO](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/ENSO.ipynb) contain the respective teleconnections analysis performed on ERA5 data.
- [NAO_cycle3/ENSO_cycle3](https://github.com/oloapinivad/AQUA/blob/main/diagnostics/teleconnections/notebooks/NAO_cycle3.ipynb) contains the plot routines to compare the teleconnections between ERA5 and the nextGEMS simulations.


## Contributing

Contributions are welcome, please open an issue or a pull request.
If you have any doubt or suggestion, please contact the AQUA team or Matteo Nurisso (m.nurisso@isac.cnr.it).
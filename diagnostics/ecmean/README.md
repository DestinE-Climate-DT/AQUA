# ECmean4 Performance Indices

Main authors: 
- Paolo Davini (CNR-ISAC, p.davini@isac.cnr.it)

## Description

This incorporates within AQUA the feature of the open-source package ECmean4. 
With this tool is possible to compute global mean and Reichler and Kim performance indices, basing the analysis on a low resolution version of the required data. Further information can be obtained from the [Official ECmean4 Documentation](https://ecmean4.readthedocs.io/en/latest/)

## Table of Contents

* [Installation Instructions](#installation-instructions)

  - [Installation on Levante](#installation-on-levante)

  - [Installation on Lumi](#installation-on-lumi)

* [Data requirements](#data-requirements)

* [Examples](#examples)

* [Contributing](#contributing)

## Installation Instructions

### Installation on Levante

To install the diagnostic on `Levante` you can use conda. However, to simplif the procedure a installation script `install_ECmean4.sh` is available in this folder and will guide through the installation. Simply run:

```bash
install_ECmean4.sh
```

A few flag are available in the script and can be manually adjusted if necessary. 
- `do_clean` is used to remove a pre-existing mamba/conda environment
- `do_pip`: you can chose if you want to install the [PyPi version](https://pypi.org/project/ECmean4/) of ECmean4 (i.e. a stable realease) or the one from Github. 
- `branch`: In case you install from GitHub (`do_pip=False`) you can select with branch point to. Mostly for development purposes. 
- `mamba`: If you have mamba installed, do not change. You can set here conda instead (not tested)


### Installation on Lumi 

Not yet tested.

## Data requirements  

ECmean4 is based on multiple variables but can be configured as a function what the user might need. Performance Indices, the key diagnostic included in AQUA, 

## Examples

The **notebook/** folder contains the following notebooks:

- **ecmean-test.ipynb**: 
  A simple test for ECmean4 usage on some of the LRA data


## Contributing

The ECmean4 development is carried out on [ECmean4 Github page](https://github.com/oloapinivad/ECmean4). Please refer to this. 


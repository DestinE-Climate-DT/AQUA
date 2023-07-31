# Dummy diagnostic

The `README_template.md` meant to be used as a `README.md` template for new diagnostics.

## Description

Concise overview that explains the purpose, key features, and benefits of your diagnostics.

## Table of Contents

* [Installation Instructions](#installation-instructions)

  - [Installation on Levante](#installation-on-levante)

  - [Installation on Lumi](#installation-on-lumi)

* [Data requirements](#data-requirements)

* [Examples](#examples)

* [Contributing](#contributing)

## Installation Instructions

Clearly explain how to install and set up your project. Include any dependencies, system requirements, or environment configurations that users should be aware of. Test installation, set up, and run your diagnostic on `Lumi` and `Levante.` If the installation requires additional dependencies, system requirements, environment configurations, or run directs specific data, you must document it. 

### Installation on Levante

To install the diagnostic on `Levante` you can use conda.
You should create an `AQUA/diagnostics/dummy/env-dummy.yml` file and include all the necessary dependencies for your diagnostic. 

To install the diagnostic in a new conda environment run:

```bash
conda env create -f env-dummy.yml
```

To install the diagnostic in an existing conda environment run:

```bash
conda env update -f env-dummy.yml
```

To activate the environment run:

```bash
conda activate dummy-diagnostic
```

or the name of the environment you chose to update.

The diagnostic environment is compatible with python 3.9 and 3.10 and with the AQUA framework.
Different diagnostic environments may be not compatible with each other.
If you want to use multiple diagnostics at the same time, it is recommended to use the different environments for each of them.

### Installation on Lumi 

Add details on the Lumi installation here

## Data requirements  

Please specify if your diagnostic can only be performed on data with particular requirements. If your diagnostics have no Data requirements, you should also document that.
If you diagnositcs requires observational/reanalysis data which is not available on Levante and/or Lumi, please contact the AQUA team providing clear specification and download path so that this data can be added to the catalog.

## Examples

The **notebook/** folder contains the following notebooks:

- **dummy.ipynb**: 
  Provide a brief description for each notebook (2-3 sentences).
- 

## Contributing

Include your contact information or any official channels (such as email, GitHub profile) through which users can reach out to you for support, questions, or feedback.


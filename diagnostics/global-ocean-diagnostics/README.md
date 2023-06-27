# Global Ocean diagnostic

The `README.md` file contains recommendations for the structure of your diagnostic. 

As a template for the actual `README.md`, please, use the `README_template.md.` in the same repository. 

## Table of Contents

* [Installation Instructions](#installation-instructions)

  - [Installation on Levante](#installation-on-levante)

  - [Installation on Lumi](#installation-on-lumi)

* [Diagnostic structure](#diagnostic-structure)

* [Code](#code)

* [Examples](#examples)

## Installation Instructions

Test installation, set up, and run your diagnostic on `Lumi` and `Levante.` If the installation requires additional dependencies, system requirements, environment configurations, or run directs specific data, you must document it.

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

## Diagnostic structure 

- **diagnostics/**: The root directory of the diagnostic.

  - **dummy/**: contains the code of the diagnostic

    - **notebooks/**: contains notebooks with examples of how to use the diagnostic

    - **data/**: contains data for the tests if needed. Please do not commit large files to git. You can add data to the `.gitignore` file if needed. Make always use of the `Reader` class and functions available in the framework to load data. If you need to add data to the catalogue, please contact the AQUA team.

    - **env-dummy.yml**: contains the dependencies for the diagnostic. It is used to create a conda environment for the diagnostic. Diagnostics should be developed in separate environments to avoid conflicts between diagnostics that may need different python packages. Always check that the diagnostic works with the latest version of the framework and eventually update the dependencies in the `env-dummy.yml` file. (If you have suggestions on how to improve this, please let us know.s)

- **tests/**

  - **dummy/**: contains tests for the diagnostic. Please add tests for all the functions you add to the diagnostic. Please load data that are needed for the tests not in the git. You may need to add data to the catalogue for the `ci` machine. If you need to add data to this catalogue, please contact the AQUA team. Tests run with a github action when you push to the repository in a pull request to the main branch. Note that the workflow file is in the `.github` folder. Modify it accordingly to your diagnostic and uncomment the lines to run the tests when you are ready. Please keep in mind that the suggested way to proceed is based on the creation of a `@pytest.mark.yourdiag` marker so that you can select only the test that you need in the workflow (see the workflow example). This marker has to be added in the `pytest.ini` file.  



- **docs/sphinx/sorce/diagnostics/dummy.rts**: contains the documentation for the diagnostic. Please add documentation for all the functions you add to the diagnostic. The documentation is built with sphinx.

## Code

Please add docstrings to all the functions you add to the diagnostic.
You can find an example of how to write the docstrings in the `dummy_class.py` file. 

Make sure that the code passes the `flake8` checks.
Take advantage as much as possible of the functions available in the framework. 

If new data or functions that you think may be useful for other diagnostics are needed, please contact the AQUA team. It may be added to the framework.


## Examples

The **notebook/** folder should contain the notebooks with clear demonstration capabilities and applications of your diagnostic. 
This notebook should be named after the diagnostic itself or a particular functionality and have an extension .ipynb.


Recommendations for notebook structure:
 - Reduce the number of packages you're importing. Try to keep all imports in your module (`dummy_class.py` and `dummy_func.py`).

 - Do not produce too long notebooks. If needed, split the notebook into a few based on diagnostic applications.

 - Split the notebook into sections and create the Table of Content. (you can find the example of it in **notebook/dummy.ipynb**)

 - Provide well-described comments to help users understand the functionality of your diagnostic.

 **Please note that notebooks may load data from the DKRZ cluster, so they may not work outside of it.**




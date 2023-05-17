# Dummy diagnostic

This is a dummy diagnostic that does nothing.
It is meant to be used as a template for new diagnostics.

## Notes for developers

Please remove these notes when you are done and adapt this `README` file.

### Folders

- `diagnostics/dummy-diagnostic`: contains the code of the diagnostic
- `diagnostics/dummy-diagnostic/notebooks`: contains notebooks with examples of how to use the diagnostic
- `tests/dummy-diagnostic/`: contains tests for the diagnostic. Please add tests for all the functions you add to the diagnostic. Please load data that are needed for the tests not in the git. You may need to add data to the catalogue for the `ci` machine. If you need to add data to this catalogue, please contact the AQUA team. Tests run with a github action when you push to the repository in a pull request to the main branch. Note that the workflow file is in the `.github` folder. Modify it accordingly to your diagnostic and uncomment the lines to run the tests when you are ready.
- `diagnostics/dummy-diagnostic/data`: contains data for the tests if needed. Please do not commit large files to git. You can add data to the `.gitignore` file if needed. Make always use of the `Reader` class and functions available in the framework to load data. If you need to add data to the catalogue, please contact the AQUA team.
- `env-dummy.yml`: contains the dependencies for the diagnostic. It is used to create a conda environment for the diagnostic. Diagnostics should be developed in separate environments to avoid conflicts between diagnostics that may need different python packages. Always check that the diagnostic works with the latest version of the framework and eventually update the dependencies in the `env-dummy.yml` file. (If you have suggestions on how to improve this, please let us know.s)
- `docs/sphinx/sorce/diagnostics/dummy-diagnostic.rts`: contains the documentation for the diagnostic. Please add documentation for all the functions you add to the diagnostic. The documentation is built with sphinx.

### Code

Please add docstrings to all the functions you add to the diagnostic.
Make sure that the code passes the `flake8` checks.
Take advantage as much as possible of the functions available in the framework. If new data or functions that you think may be useful for other diagnostics are needed, please contact the AQUA team. It may be added to the framework.

## Installation

To install this diagnostic you can use conda.
The `environment.yml` file contains the dependencies for this diagnostic.
It is located in `AQUA/diagnostics/dummy-diagnostic/env-dummy.yml`.

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

## Examples

The `notebooks` folder contains notebooks with examples of how to use the diagnostic and its main functions.
Please note that notebooks may load data from the DKRZ cluster, so they may not work outside of it.


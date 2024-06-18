# Dummy diagnostic

The `README.md` file contains recommendations for the structure of a dummy diagnostic, that should be used to develop your diagnostic. 

As a template for the actual `README.md`, please use the `README_template.md` in this same directory. 

## Diagnostic structure 

This list all the files you will find in the dummy diagnostic example. This folder structure should be followed when developing your code.

- **diagnostics/**: The root directory of the diagnostic.

  - **dummy/**: contains the code of the diagnostic

    - **notebooks/**: contains notebooks with examples of how to use the diagnostic. Two different examples are provided, making use of the two different test classes (see below in the Code section)

    - **data/**: contains data for the tests if needed. Please do not commit large files to git. You can add data to the `.gitignore` file if needed. Make always use of the `Reader` class and functions available in the framework to load data. If you need to add data to the catalog, please contact the AQUA team.

    - **cli/**: contains the command line interface for your diagnostic. This should be a python executable script, which can be configured from an external yaml file and used to run the diagnostic on one or more experiments without the need of using a specific notebook. If you computation is heavy so that this cannot be done on a login node, please provide also a batch job that can be submitted to SLURM. 

    - **env-dummy.yml**: contains the dependencies for the diagnostic. It is used to create a conda environment for the diagnostic. Diagnostics should be developed in separate environments to avoid conflicts between diagnostics that may need different python packages. Always check that the diagnostic works with the latest version of the framework and eventually update the dependencies in the `env-dummy.yml` file. (If you have suggestions on how to improve this, please let us know.s)

- **tests/**

  - **dummy/**: contains tests for the diagnostic. Please add tests for all the functions you add to the diagnostic. Please load data that are needed for the tests not in the git. You may need to add data to the catalog for the `ci` catalog. If you need to add data to this catalog, please contact the AQUA team. Tests run with a github action when you push to the repository in a pull request to the main branch. Note that the workflow file is in the `.github` folder. Modify it accordingly to your diagnostic and uncomment the lines to run the tests when you are ready. Please keep in mind that the suggested way to proceed is based on the creation of a `@pytest.mark.yourdiag` marker so that you can select only the test that you need in the workflow (see the workflow example). This marker has to be added in the `pytest.ini` file.  

- **docs/sphinx/sorce/diagnostics/dummy.rts**: contains the documentation for the diagnostic. Please add documentation for all the functions you add to the diagnostic. The documentation is built with sphinx.

## Code

A function-based approch is mandatatory. Usage of classes to wrap together the different functions is encouraged. Two different examples of classes are presented in this folder: 1. `DummyDiagnostic()` is a simple where the `Reader` capabilities are not included in the code while  2. `DummyDiagnosticWrapper()` incorporates the `Reader` call inside the class itself. Both approaches are possible and you are encouraged to choose the one the most fit your code. 
  
Please add docstrings to all the functions you add to the diagnostic. We are following [Google-style docstring](https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html).
You can find an example of how to write the docstrings in the `dummy_class_reader.py` or in `dummy_func.py` file. 

Make sure that the code passes the basic `flake8` checks. To improve the code format you can use `autopep8`.
Take advantage as much as possible of the functions available in the framework. 

If new data or functions that you think may be useful for other diagnostics are needed, please contact the AQUA team. 
It may be added to the framework.


## Examples

The **notebook/** folder should contain the notebooks with clear demonstration capabilities and applications of your diagnostic. 
This notebook should be named after the diagnostic itself or a particular functionality and have an extension .ipynb.


Recommendations for notebook structure:
 - Reduce the number of packages you're importing. Try to keep all imports in your module (`dummy_class_reader.py`, or `dummy_class_timeband.py`, and `dummy_func.py`).

 - Do not produce too long notebooks. If needed, split the notebook into a few based on diagnostic applications.

 - Split the notebook into sections and create the Table of Content. (you can find the example of it in **notebook/dummy_class_timeband.ipynb**)

 - Provide well-described comments to help users understand the functionality of your diagnostic.

 **Please note that notebooks may load data from the DKRZ cluster, so they may not work outside of it.**




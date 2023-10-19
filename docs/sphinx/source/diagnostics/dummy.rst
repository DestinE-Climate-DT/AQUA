Dummy
================

Dummy provides an example on how the documentation for diagnostics of the AQUA framework should be organised and written.
This documentation provides also guidelines on how a diagnostic developer should write documentation about their diagnostic,
covering a short description of its contents and of its scientific basis. 
Please try to adhere to the suggested scheme as far as possible.

Description
-----------

Please write here in a clear and concise manner what the diagnostic is about 
and what it is supposed to be doing and the motivation behind it (i.e. which phenomenon or physical process 
is diagnosed and why it is important in the context of the analysis of high-resulution climate simulations).
In case the diagnostic is based on some already published material it is strongly 
recommended to include references to the inherent literature (we have a reference list below).


Structure
-----------

The dummy diagnostic follows a class structure and consists of the files:

* ``dummy_class.py``: a python file in which the DummyDiagnostic class constructor and the other class methods are included;
* ``dummy_func.py``: a python file which contains functions that are called and used in the dummy class;
* ``notebooks/dummy.ipynb``: an ipython notebook which uses the dymmy class and its methods;
* ``cli/cli_dummy.py``: a python file which contains the command line interface (CLI) of the dummy diagnostic;
* ``README.md`` : a readme file which contains some tecnical information on how to install the dummy diagnostic and its environment. 

Input variables example
------------------------

* ``tprate`` (total precipitation rate, GRIB paramid 260048)
* ``2t``     (2 metre temperature, GRIB pramid 167)

Output 
------

List here types of files/datasets produced by the diagnostic.
Please keep in mind that diagnostic output should be both figures (PDF format is recommended)
and data (NetCDF file is recommended). 

Methods used
------------

Examples from the DummyDiagnostic class contained in the ``dummy_class.py`` file:

* "DummyDiagnostic": the Dummy diagnostic class;
* "retrieve": method to retrieve the data from the Reader class;
* "fldmean": method to compute the field mean of the retrieved data;
* "multiplication": method to compute the multiplication of the retrieved data. 
                    It is an example of method that uses of external functions of the module dummy_func

Note that it is important to add docstrings to each method.
We are following `Google-style docstring <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_

.. note::
    Please note that there is no need to list all the methods used, but the most important which are exposed to the users should be presented

Functions used
--------------

Example of functions contained in the dummy_func.py file:

* "dummy_func": dummy function used in the dummy class.

Note that it is important to add docstrings to each function.
We are following `Google-style docstring <https://sphinxcontrib-napoleon.readthedocs.io/en/latest/example_google.html>`_

.. note::
    Please note that there is no need to list all the functions used, but the most important which are exposed to the users should be presented

Observations
------------

If relevant, list the observational datasets used by this diagnostic (e.g. for validation/comparison).
Some examples are ERA5 reanalysis, CERES, MSWEP etc...

References
----------

* E. Empty, D. Dummy et al. (2023) The art of saying nothing. Emptyness, 1: 0-1. `DOI <http://doi.org/00.0000/e-00000-000.xxxx>`_


Example Plot(s)
---------------

.. figure:: figures/dummy-diagnostic1.png
    :width: 10cm

    An illustration of the big void left by this diagnostic

Available demo notebooks
------------------------

Notebooks are stored in diagnostics/dummy-diagnostic/notebooks

* `dummy_class_readerwrapper.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/dummy/notebooks/dummy_class_readerwrapper.ipynb>`_
* `dummy_class_timeband.ipynb <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/dummy/notebooks/dummy_class_timeband.ipynb>`_
        
Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "dummy" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: dummy
    :members:
    :undoc-members:
    :show-inheritance:

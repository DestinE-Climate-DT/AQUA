.. _example_use_case:

Example use case
================

If you have not followed the :ref:`getting_started` guide, please consider doing so before proceeding with this example.

We suppose that AQUA is installed in our favourite machine and we have added a catalog.

.. note::

    The following example use the ``climate-dt-phase1`` catalog, available on Lumi HPC.

We will explore here a little example using the AQUA framework.
No diagnostic is involved here, this can be found in the :ref:`diagnostics` section.

We will walk you through an example using AQUA to interpolate atmospherically
temperature data to 1°x1° grid, plot a timestep of it and
then calculate the mean global temperature time series on the original grid.
This can be done in a few lines of code and using a Jupyter notebook.

Let's start with retrieving the data from the catalog.
First we import the ``Reader`` class from the ``aqua`` package.

.. code-block:: python

    from aqua import Reader

We then instantiate the ``Reader`` object.
To access a catalog entry, a three layer structure is used: ``model``, ``exp`` and ``source``.
While doing so we specify the target grid to which we want to interpolate the data
and we turn on fixing of the data, so that the data are delivered in a common format.
Notice that ``fix=True`` is the default option, so we could have omitted it.

.. code-block:: python

    reader = Reader(model="IFS-NEMO", exp="historical-1990", source="hourly-hpz7-atm2d",
                    regrid='r100', fix=True)

This will create a ``Reader`` object that will allow us to access the data from the catalog.
Data are not retrieved yet at this stage and eventually we can specify variables and time range while accessing the data.

We now retrieve the data.

.. code-block:: python

    data = reader.retrieve()

We are asking for the data to be retrieved and a xarray is returned,
so that only metadata are loaded into memory.
This allows us to retrieve blindly the data, without worrying about the size of the data.
We can then, in the development stage, explore the data and see what we have.
In a production environment instead, AQUA can be used to retrieve only variables and time ranges of interest.

.. note::
  Data are retrieved as an xarray object, specifically a ``xarray.Dataset``, even in the case we asked for a single variable.

We can now interpolate the data to a 1°x1° grid and plot a timestep of it, all with AQUA tools.

.. code-block:: python

    data_2t_r = reader.regrid(data['2t']) # This is an xarray.DataArray
    data_2t_r.isel(time=0).aqua.plot_single_map()

We obtain as image:

.. image:: figures/getting_started_map.png
    :width: 500
    :align: center

We used the regrid method to interpolate the data to a 1°x1° grid, with preprocessing of the weights already done
while initializating the Reader.
We then used the ``plot_single_map()`` function to plot the first timestep of the data.
This function has been used as accessor but can also be called as a standalone function.
See :ref:`accessors` for more information.

We can now calculate the mean global temperature time series on the original grid.
We will then go back to use the original data, without regridding them,
to show area evaluation capabilities of AQUA.

.. code-block:: python

    global_mean = reader.fldmean(data['2t'].isel(time=slice(100,200)))
    global_mean.plot()

We obtain as image:

.. image:: figures/getting_started_timeseries.png
    :width: 500
    :align: center

For more detailed examples and tutorials, refer to the :ref:`examples_tutorials` section of this documentation
or explore the Jupyter notebooks provided with AQUA.

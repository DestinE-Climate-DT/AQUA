Graphics 
========

The aqua.graphics module provides a simple function to easily plot a map of a variable.
A function called `plot_single_map` is provided with many options to customize the plot.

Example of usage
----------------

The function takes as input an xarray.DataArray, with a single timestep to be selected
before calling the function. The function will then plot the map of the variable and,
if no other option is provided, will adapt colorbar, title and labels to the attributes
of the input DataArray.

In the following example we plot an sst map from the first timestep of ERA5 reanalysis:

.. code-block:: python
    
    from aqua import Reader, plot_single_map

    reader = Reader(model='ERA5', exp='era5', source='monthly')
    sst = reader.retrieve(var=["sst"])
    sst_plot = sst["sst"].isel(time=0)

    plot_single_map(sst_plot, title="Example of a custom title", filename="example",
                    outputdir=".", format="png", dpi=300, save=True)

This will produce the following plot:

.. figure:: figures/single_map_example.png
    :align: center
    :width: 100%

    Example of the above code.

Available demo notebooks
------------------------

- `single_map.ipynb` in the `notebooks/graphics` folder
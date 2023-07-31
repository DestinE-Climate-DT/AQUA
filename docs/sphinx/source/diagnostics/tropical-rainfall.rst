Tropical rainfall diagnostic
============================

The precipitation variability is an excellent indicator of the accuracy of climatological simulations.
The goal of tropical rainfall diagnostic is to provide fast and straightforward precipitation analysis in tropical or global areas. 


Description
-----------

The current version of tropical rainfall diagnostic successfully achieves the minimal requirements: 

* calculation of histograms for selected tropical area band,
* calculation of the seasonal mean of precipitation along the latitude or longitude coordinate, and
* calculation of the bias between the climatological model and observations.

The diagnostic also provides us with simple in-the-use plotting functions to create a graphical representation of obtained results. 


Structure
---------

The tropical-rainfall diagnostic follows a class structure and consists of the files:

* `tropical_rainfall_class.py`: a python file in which the **Tropical_Rainfall** class constructor and the other class methods are included;
* `tropical_rainfall_func.py`: a python file which contains functions that are called and used in the tropical-rainfall class;
* `env-tropical-rainfall.yml`: a yaml file with the required dependencies for the tropical-rainfall diagnostic;
* `notebooks/*.ipynb`: an ipython notebook which uses the dymmy class and its methods;
* `README.md` : a readme file which contains some tecnical information on how to install the tropical-rainfall diagnostic and its environment. 


Input variables
---------------
* `tprate` (total precipitation rate, GRIB paramid 260048)

Output
------
All output of the diagnostic is in the format of NetCDF to be further analysed, or in PDF for rapid visualization. 

Examples
--------

The histogram calculation
^^^^^^^^^^^^^^^^^^^^^^^^^

The most straightforward illustration of a histogram calculation with continuous uniform binning:

.. code-block:: python

      diag = Tropical_Rainfall(num_of_bins = 20, first_edge = 0, width_of_bin = 1*10**(-5))
      diag.histogram(ifs)

  
The output of the histogram function is xarray.Dataset, which has two coordinates 

* `center_of_bin`:   the center of each bin

* `width`:           width of each bin

The xarray.Dataset  contains three variables: `counts`, `frequency` (the number of cases in each bin, normalized by the total number of counts), 
and `pdf` (the number of cases in each bin, normalized by the total number of counts and width of each bin). 
The obtained xarray.Dataset contains both local and global attributes. 
Global attribute `history` and local attributes  contains the information about time and space grid for which the diagnostic performed calculations: `time_band`, `lat_band`, and  `lon_band`.  

The diagnostic provides unique names for the NetCDF files which contain the histogram.  
Namely, the file's name includes the first and last time steps, for which the diagnostic does the calculations

List of histograms 
^^^^^^^^^^^^^^^^^^

The diagnostic can combine any number of histograms into a single histogram, recalculating 
the frequencies and pdf values and modifying the attributes automatically.

For example, if you want to merge all histograms if the specified repository, set the following flag: **all=True**.

.. code-block:: python

  path_to_histograms="/work/bb1153/b382267/tropical_rainfall_cicle3/NetCDF/histograms/"

  merged_histograms = diag.merge_list_of_histograms(path_to_histograms=path_to_histograms, all=True)

**Reminder**: Store the obtained histograms for distinct models in separate repositories to avoid possible errors. 


The histogram plots 
^^^^^^^^^^^^^^^^^^^

The diagnostic contains the simple in-the-use function to create the histogram plot. 
The user can create plots of the obtained data in  different styles and scales. 
The example of a histogram plot is:

.. code-block:: python

  diag.histogram_plot(histogram, smooth = False, color_map = 'gist_heat', figsize=0.7, 
               xlogscale = True, ylogscale=True)


You can find an example of the histogram obtained with the tropical-rainfall diagnostic below. 

.. figure:: figures/trop_rainfall_icon_ngc3028_ifs_tco2559_ng5_ifs_tco1279_orca025_mswep_lra_r100_monthly_comparison_histogram.png
    :width: 12cm

Seasonal Mean Values
^^^^^^^^^^^^^^^^^^^^

The diagnostic can provide us with a graphical comparison of the mean value along different coordinates. 
For example, the function 

.. code-block:: python

  diag.trop_lat = 90
  diag.mean_and_median_plot(icon_ngc3028, coord='lon',  
                                  legend='icon, ngc3028', new_unit = 'mm/day' )

calculates the mean value of precipitation along the longitude during 

- December-January-February (`DJF`), 
- March-April-May (`MAM`), 
- June-July-August (`JJA`), 
- September-October-November (`SON`), and 
- for the total period of time. 

Bias between model and observations 
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Tropical-rainfall diagnostic provides the graphical representation of the bias between the mean value of precipitation of the 
climatological model and the mean value of observations. 
The function 

.. code-block:: python

  diag.plot_bias(icon_ngc3028, dataset_2 = mswep_mon, seasons=True, new_unit='mm/day',  trop_lat=90,  vmin=-10, vmax=10,
                    plot_title='The bias between icon, ngc3028 ans mswep, monthly, 1 degree res (100km)',
                    path_to_pdf=path_to_pdf, name_of_file='icon_ngc3028_mswep_lra_r100_monthly_bias')

calculates the mean value of precipitation for each season  `DJF`, `MAM`, `JJA`, `SON` and  for the total period of time.

Available demo notebooks
------------------------

The notebook folder contains the demonstration of:

#. `Histogram Calculation <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/histogram_calculation.ipynb>`_: 
   
   The notebook demonstrates the major abilities of tropical rainfall diagnostic: 
    - initialization of an object of the diagnostic class, 
    - selection of the class attributes,  
    - calculation of the histograms in the form of xarray, 
    - saving the histograms in the storage,
    - and loading the histograms from storage.

#. `Histogram Plotting <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/histogram_plotting.ipynb>`_:

   The notebook demonstrates the abilities of the histogram plotting functions:
    - selection of the plot style: step line style, 2D smooth line style, and different color maps,
    - selection of the plot size, axes scales, 
    - saving plot into storage, 
    - plotting the counts, frequencies, and Probability density function (pdf) from the obtained histograms.

#. `Diagnostic During Streaming <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/diagnostic_vs_streaming.ipynb>`_:

   The notebook demonstrates the usage of diagnostic during the streaming mode:
    - saving the obtained histogram with the histogram into storage per each chunk of any data during the stream, 
    - loading all or multiple histograms from storage and merging them into a single histogram. 

#. `Comparison of Low-Resolution Cicle3 Models <https://github.com/oloapinivad/AQUA/blob/main/diagnostics/tropical_rainfall/notebooks/comparison_of_lowres_models.ipynb>`_:

   The notebook demonstrates:
    - histogram comparison for different climate models,
    - the ability to merge a few separate plots into a single one, 
    - mean of tropical and global precipitation calculations for different climate models,
    - bias between climatological model and observations. 

Detailed API
------------

This section provides a detailed reference for the Application Programming Interface (API) of the "tropical_rainfall" diagnostic,
produced from the diagnostic function docstrings.

.. automodule:: tropical_rainfall
    :members:
    :undoc-members:
    :show-inheritance:
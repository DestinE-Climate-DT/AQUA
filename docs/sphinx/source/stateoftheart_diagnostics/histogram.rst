Histogram
=========

Description
-----------

The Histogram diagnostic computes and plots histograms or probability density functions (PDFs) 
of climate variables over specified regions.
The diagnostic supports both **raw histograms** (counts per bin) and **normalized PDFs** 
(probability density functions with integral = 1).
Histograms can be computed over specific geographic regions, with default regions available or 
custom regions definable in the configuration file.
Optional latitudinal weighting accounts for grid cell area variations.

Classes
-------

There are two main classes for computing and plotting histograms:

* **Histogram**: Computes histograms or PDFs of climate variables.
  
  - Supports **raw histograms** (counts) and **normalized PDFs** (``density=True``)
  - Optional **latitudinal weighting** to account for grid cell area
  - Customizable **bin count** and **range** for histogram computation
  - Regional analysis with predefined or custom regions

* **PlotHistogram**: Produces publication-quality line plots of computed histograms/PDFs.
  
  - Single or multi-model comparison plots
  - Optional reference dataset overlay
  - Logarithmic scales for x and y axes
  - Optional smoothing with configurable window size
  - Customizable axis limits

.. note::

    The diagnostic computes histograms over the **entire temporal period** specified 
    (no seasonal decomposition).

Getting Started
---------------

**File locations:**

* Diagnostic code: ``src/aqua_diagnostics/histogram/``
* Region definitions: ``config/tools/histogram/definitions/regions.yaml``
* Example notebook: ``notebooks/diagnostics/histogram/``
* Config template: ``templates/diagnostics/config-histogram.yaml``

**Supported variables:**

The diagnostic works with climate variables on regular latitude-longitude grids:

* **Direct variables**: ``tprate`` (precipitation), ``2t`` (temperature), ``sst`` (sea surface 
  temperature), etc.
* **Derived variables**: Using ``EvaluateFormula`` syntax (e.g., ``2t - 273.15`` for °C)

**Supported regions:**

``global`` (or ``null``), ``tropics``, ``europe``, ``nh`` (Northern Hemisphere), 
``sh`` (Southern Hemisphere).

Basic usage
-----------

The recommended way to use this diagnostic is through the Python API, as shown in the 
notebook below.

**Minimal example:**

.. code-block:: python

    from aqua.diagnostics.histogram import Histogram, PlotHistogram

    # Compute histogram/PDF
    hist = Histogram(
        catalog='climatedt-phase1',
        model='ICON',
        exp='historical-1990',
        source='lra-r100-monthly',
        startdate='1990-01-01',
        enddate='1999-12-31',
        bins=100,
        weighted=True
    )
    hist.run(var='tprate', units='mm/day', density=True)
    
    # Plot PDF
    plot = PlotHistogram(data=[hist.histogram_data])
    plot.run(outputdir='./', ylogscale=True)

**For multi-model comparisons or reference data**, see the detailed examples in the section below.

Available demo notebooks
------------------------

📓 **Single histogram/PDF plot** → `histogram.ipynb <https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/diagnostics/histogram/histogram.ipynb>`_

   Learn the basics: compute histograms/PDFs, compare with observations, customize plots

**Key concepts covered:**

- Histogram vs PDF: ``density=False`` (counts) vs ``density=True`` (probability density)
- Latitudinal weighting: ``weighted=True`` for area-corrected distributions
- Bin configuration: ``bins`` (number) and ``range`` (min/max) parameters
- Plot customization: log scales (``xlogscale``, ``ylogscale``), smoothing, axis limits
- Regional selection and custom regions

CLI usage
---------

For batch processing or automation, the diagnostic can be run via CLI using a configuration file:

.. code-block:: bash

    # Copy and customize the template
    cp templates/diagnostics/config-histogram.yaml my_config.yaml
    
    # Run diagnostic
    python src/aqua_diagnostics/histogram/cli_histogram.py \
        --config my_config.yaml \
        --model ICON \
        --exp historical-1990 \
        --loglevel INFO

**Key CLI arguments:**

``--config``, ``--model``, ``--exp``, ``--catalog``, ``--source``, ``--regrid``, 
``--realization``, ``--outputdir``, ``--startdate``, ``--enddate``, ``--loglevel``, ``--nworkers``

For the complete list of arguments, run:

.. code-block:: bash

    python src/aqua_diagnostics/histogram/cli_histogram.py --help

.. note::

    **Suggested workflow**: Copy the template 
    (``cp templates/diagnostics/config-histogram.yaml my_config.yaml``), customize it with 
    your parameters, and run with ``--config my_config.yaml``.
    
    **Quick testing**: CLI arguments (``--model``, ``--exp``, etc.) can override config file 
    values without editing the file, useful for rapid experimentation.
    
    For most use cases, we recommend the **programmatic approach** (notebooks) rather than CLI.

Configuration file structure
----------------------------

The template (``templates/diagnostics/config-histogram.yaml``) defines datasets, 
reference data, and diagnostic parameters:

**Basic structure:**

.. code-block:: yaml

    # Dataset(s) to analyze
    datasets:
      - catalog: 'climatedt-phase1'
        model: 'ICON'
        exp: 'historical-1990'
        source: 'lra-r100-monthly'
        startdate: '1990-01-01'
        enddate: '1999-12-31'
    
    # Reference dataset (optional)
    references:
      - catalog: 'obs'
        model: 'ERA5'
        exp: 'era5'
        source: 'monthly'
        startdate: '1990-01-01'
        enddate: '1999-12-31'
    
    # Output settings
    output:
      outputdir: "./"
      save_pdf: true
      save_png: true
      dpi: 300
    
    # Diagnostic configuration
    diagnostics:
      histogram:
        run: true
        bins: 100                    # Number of bins
        range: null                  # [min, max] or null for auto
        weighted: true               # Use latitudinal weights
        density: true                # Compute PDF (normalized)
        xlogscale: false             # Log scale for x-axis
        ylogscale: true              # Log scale for y-axis
        smooth: false                # Apply smoothing
        variables:
          - name: 'tprate'
            units: 'mm/day'
            regions: ['global', 'tropics']

**Multiple datasets example** (for multi-model comparison):

.. code-block:: yaml

    datasets:
      - catalog: 'climatedt-phase1'
        model: 'ICON'
        exp: 'historical-1990'
        source: 'lra-r100-monthly'
        startdate: '1990-01-01'
        enddate: '1999-12-31'
      
      - catalog: 'climatedt-phase1'
        model: 'IFS-NEMO'
        exp: 'historical-1990'
        source: 'lra-r100-monthly'
        startdate: '1990-01-01'
        enddate: '1999-12-31'

**Variable-specific parameters:**

.. code-block:: yaml

    diagnostics:
      histogram:
        variables:
          - name: 'tprate'
            regions: ['global']
            range: [0, 20]           # Custom range for this variable
            bins: 50                 # Override global bins setting
            lon_limits: [-180, 180]  # Optional spatial constraints
            lat_limits: [-60, 60]

**Derived variables** (using formulas):

.. code-block:: yaml

    diagnostics:
      histogram:
        formulae:
          - name: 'temp_celsius'
            formula: '2t - 273.15'
            units: '°C'
            long_name: 'Temperature in Celsius'
            regions: ['global', 'tropics']

For the complete template with all available options, see 
``templates/diagnostics/config-histogram.yaml``.

Outputs
-------

The diagnostic generates:

📊 **Plots** (PDF and/or PNG):

  - Histogram/PDF line plots
  - Multi-model comparisons with reference data
  - Optional smoothing and custom axis limits

📁 **NetCDF files**:

  - Computed histogram data with bin centers and counts/densities
  - Metadata preserved from original variables

**Naming convention:**

``histogram.<diagnostic>.<catalog>.<model>.<exp>.<realization>.<var>.nc``

``histogram.<diagnostic>_pdf.<catalog>.<model>.<exp>.<realization>.<var>.<format>``

**Example:**

``histogram.histogram_pdf.climatedt-phase1.ICON.historical-1990.r1.tprate.png``

Example plots
-------------

.. figure:: figures/histogram_tprate_global.png
   :width: 100%

   Probability density function (PDF) of precipitation rate (mm/day) for the global region, 
   showing ICON model output compared to ERA5 reference data.

Reference datasets
------------------

Common reference datasets:

* **ERA5**: ECMWF's fifth generation reanalysis for global climate
* **MSWEP**: Multi-Source Weighted-Ensemble Precipitation dataset
* **BERKELEY-EARTH**: Berkeley Earth Surface Temperature dataset

Authors and contributors
------------------------

This diagnostic is maintained by Marco Cadau (@mcadau, marco.cadau@polito.it), member of 
the AQUA team.

Contributions are welcome — please open an issue or pull request. For questions, contact 
the AQUA team or the maintainer.

Developer Notes
---------------

**Internal structure:**

The diagnostic uses a three-step process:

1. **Data retrieval** via ``Reader`` from catalog:
   
   - Applies temporal and spatial selection
   - Handles unit conversion if needed

2. **Histogram computation** via ``aqua.histogram.histogram()``:
   
   - Optional latitudinal weighting: ``weights = cos(lat)``
   - Bin calculation: NumPy or Dask histogram
   - Normalization: if ``density=True``, integrates to 1

3. **Storage** as xarray DataArray:
   
   - Dimension: ``center_of_bin`` (bin centers)
   - Coordinate: ``width`` (bin widths)
   - Attributes: preserves original variable metadata

**Data attributes:**

Metadata attached to histogram DataArrays:

- ``AQUA_catalog``, ``AQUA_model``, ``AQUA_exp``: Data provenance
- ``AQUA_region``: Selected region name
- ``size_of_the_data``: Original data size
- ``units``: ``'counts'`` or ``'probability density'``
- Standard CF attributes: ``long_name``, ``standard_name``

**Graphics function:**

* ``plot_histogram()``: Line plot with flexible styling
  
  - Handles single or multiple DataArrays
  - Supports reference data overlay
  - Optional smoothing with moving average
  - Logarithmic scales for both axes
  - Auto-detects bin centers and values

**Data flow:**

1. ``Histogram.retrieve()`` → Get data from catalog
2. ``Histogram.compute_histogram()`` → Call ``aqua.histogram.histogram()``
3. ``Histogram.save_netcdf()`` → Save processed data
4. ``PlotHistogram.__init__()`` → Load data and metadata
5. ``PlotHistogram.run()`` → Create and save plots

**Smoothing algorithm:**

Simple moving average with edge handling:

.. code-block:: python

    kernel = np.ones(window_size) / window_size
    smoothed = np.convolve(data, kernel, mode='same')

**Latitudinal weighting:**

Accounts for decreasing grid cell area toward poles:

.. code-block:: python

    weights = np.cos(np.radians(lat))

API Reference
-------------

.. automodule:: aqua.diagnostics.histogram
    :members:
    :undoc-members:
    :show-inheritance:
.. _fixer:

Fixer functionalities
---------------------

The need of comparing different datasets or observations is very common when evaluating climate models.
However, datasets may have different conventions, units, and even different names for the same variable.
AQUA provides a fixer tool to standardize the data and make them comparable.

The general idea is to convert data from different models to a uniform file format
with the same variable names and units.
The default convention for metadata (for example variable names) is **GRIB**.

The fixing is done by default when we initialize the ``Reader`` class, 
using the instructions in the ``config/fixes`` folder. The ``config/fixes`` folder contains fixes in YAML files.
A new fix can be added to the folder and the filename can be freely chosen.
By default, fixes files with the name of the model or the name of the DestinE project are provided.

Concept
^^^^^^^

The fixer performs a range of operations on data:

- adopts a **common data model** for coordinates:
  names of coordinates and dimensions (lon, lat, plev, depth, etc.),
  coordinate units and direction, name (and meaning) of the time dimension. (See :ref:`coord-fix` for more details)
- **changes variable names and metadata**.
  The fixer can match the available and target variables with a convention table (see :ref:`convention-structure`)
  and assign a GRIB ParamID to the target variable, in order to automatically retrieve the metadata.
  The fixer can also use the metadata with their ShortNames and ParamID making use of the ECMWF and WMO eccodes tables.
- **derives new variables** executing trivial operations as multiplication, addition, etc. (See :ref:`metadata-fix` for more details)
  In particular, it derives from accumulated variables like ``tp`` (in mm), the equivalent mean-rate variables
  (like ``tprate`` in kg m-2 s-1). (See :ref:`metadata-fix` for more details)
- using the ``metpy.units`` module, it is capable of **guessing some basic units conversions**.
  In particular, if a density is missing, it will assume that it is the density of water and will take it into account.
  If there is an extra time unit, it will assume that division by the timestep is needed. 

Since v0.13 the fixer is split in two dictionaries that can be merged together, the ``convention`` and the ``fixer_name``.
We describe in the following sections the structure of the two dictionaries and in which files they should be placed.

.. warning::

    Other fixes at individual source level are still supported but will be deprecated in the future.
    Also the usage of a ``model-default`` as fallback fixer_name is deprecated in favour of the new approach.

.. _convention-structure:

Convention file structure
^^^^^^^^^^^^^^^^^^^^^^^^^

Since v0.13, the fixes can be complementd by a convention file that is built to be shared for any source that uses the same convention.
The convention file is a YAML file that is placed in the ``config/fixes`` folder and is named as ``convention-<convention_name>.yaml``.
At the moment the only convention available is the ``eccodes`` convention, so that the file is named ``convention-eccodes.yaml``.

A skeleton of the convention file is the following:


.. code-block:: yaml

    convention_name:
        eccodes-2.39.0:
            convention: eccodes
            version: 2.39.0
            description: 'Target variables according to GRIB2 encoding and eccodes. Shortname changes due to statistical processing (avg) are dropped.'

            vars:
            #### INSTANTANEOUS VARIABLES ####
                2d: # 2 meter dewpoint temperature
                    source:
                    - 168 # original GRIB2 code
                    - 235168
                    - 2d
                    - avg_2d
                    grib: 168

- **convention_name** is the main block, where this and other conventions can be defined.
  ``eccodes-2.39.0`` is the name of the convention, where at the moment we are hardcoding the ecCodes version (2.39.0).
- **convention**: the name of the convention.
- **version**: the version of the convention. This is not used yet in the code.
- **description**: a brief description of the convention. This is not used yet in the code.
- **vars**: the main block where the variables are defined.

Each variable is defined by a block with the following keys:

- **2d**: the name of the variable. This is the target name, so that this will be the final name that the user can ask for and use in their diagnostics.
- **source**: a list of possible sources for the variable. This can be the GRIB2 code, the shortname, the name of the variable in the source, etc.
  The fixer will look for the variable in the source and will rename it to the target name.
- **grib**: the GRIB2 code of the target variable. This is used to retrieve the metadata from the eccodes tables.
  This is also used to set the target units and trigger possible units conversion.

.. warning::

    Even if no ``fixer_name`` is defined in your source, this convention file will be used to fix the variables.
    If you want to deactivate, you can set ``fixer_name: false`` in the source metadata.

.. _fix-structure:

Fixer file structure
^^^^^^^^^^^^^^^^^^^^

The fixer file is a YAML file that contains a list of fixes.
This is a second dictionary that can be merged with the convention file or used alone.
It should be used to specify the fixes that are specific to a source or a group of sources or to add details to the convention file (e.g. decumulation, derived variables, etc.).

.. warning::

    In this implementation the merge with the convention file is done only if a block ``convention: eccodes`` is present in the fixer file.
    This allows backward compatibility with the old implementation, where no convention file was present.

Here we show an example of a fixer file, including all the possible options:

.. code-block:: yaml

    fixer_name:
        documentation-mother: 
            delete: 
                - bad_variable
            vars:
                mtpr:
                    source: tp
                    grib: true
        documentation-fix:
            parent: documentation-to-merge
            convention: eccodes
            dims:
                cells:
                    source: cells-to-rename
            coords:
                time:
                    source: time-to-rename
            deltat: 3600 # Decumulation info
            jump: month
            vars:
                2t:
                    source: 2t
                    attributes: # new attribute
                        donald: 'duck'
                mtntrf: # Auto unit conversion from eccodes
                    derived: ttr
                    grib: true
                    decumulate: true     
                2t_increased: # Simple formula
                    derived: 2t+1.0
                    grib: true
                # example of derived variable, should be double the normal amount
                mtntrf2:
                    derived: ttr+ttr
                    src_units: J m-2 # Overruling source units
                    decumulate: true  # Test decumulation
                    units: "{radiation_flux}" # overruling units
                    mindate: 1990-09-01T00:00 # setting to NaN all data before this date
                    attributes:
                        # assigning a long_name
                        long_name: Mean top net thermal radiation flux doubled
                        paramId: '999179' # assigning an (invented) paramId

We put together many different fixes, but let's take a look at the 
different sections of the fixer file.

- **documentation-fix**: This is the name of the fixer. We refer to it as ``fixer_name``.
  It is used to identify the fixer and will be used in the entry metadata to specify which fixer to use. (See :ref:`add-data` for more details)
- **parent**: a source ``fixer_name`` with which the current fixes have to be merged. 
  In the above example, the ``documentation-fix`` will extend the ``documentation-mother`` fix integrating it. 
  Notice that this is another ``fixer_name``, so that if the convention is specified in one of the two, it will be used as well.
- **convention**: the name of the convention to be used. This is used to merge the convention file with the fixer file.
  If this key is not present, the fixer will not be merged with the convention file.
- **data_model**: the name of the data model for coordinates. The default is ``aqua`` convention (See :ref:`coord-fix`).
- **coords**: extra coordinates handling if data model is not flexible enough. (See :ref:`coord-fix`).
- **dims**: extra dimensions handling if data model is not flexible enough.  (See :ref:`coord-fix`).
- **decumulation**: 
    - If only ``deltat`` is specified, all the variables that are considered as cumulated flux variables 
      (i.e. that present a time unit mismatch from the source to target units) will be divided
      by ``deltat``. This is done automatically based on the values of target and source units.
      ``deltat`` can be an integer in seconds, or alternatively a string with ``monthly``: in this case
      each flux variable will be divided by the number of seconds of each month. Please notice that from v0.13
      it is possible to specify the ``deltat`` in the metadata of the source. This will have the priority over the fixer definition.
    - If additionally ``decumulate: true`` is specified for a specific variable,
      a time derivative of the variable will be computed.
      This is tipically done for cumulated fluxes for the IFS model, that are cumulated on a period longer
      than the output saving frequency.
      The additional ``jump`` parameter specifies the period of cumulation.
      Only months are supported at the moment, implying that fluxes are reset at the beginning of each month.
- **timeshift**: Roll the time axis forward/back in time by a certain amount. This could be an integer that will
  be interpreted as a number of timesteps, or a pandas Timedelta string (e.g. ``1D``). Positive numbers
  will move the time axis forward, while negative ones will move it backward (e.g. ``-2H``). Please note that only the 
  time axis will be affected, the Dataset will maintain all its properties. 
- **vars**: this is the main fixer block, described in detail on the following section :ref:`metadata-fix`.
- **delete**: a list of variable or coordinates that the users want to remove from the output Dataset

.. _aqua-convention:

AQUA variables convention
^^^^^^^^^^^^^^^^^^^^^^^^^

Based on the convention file described in :ref:`convention-structure`, we have defined a convention for the AQUA variables.
This means that all the experiments maintained by the AQUA project will have the same target variable names if the fixer is activated.

The convention file is named ``convention-eccodes.yaml`` and is placed in the ``config/fixes`` folder or available at `this link <https://github.com/DestinE-Climate-DT/AQUA/blob/main/config/fixes/convention-eccodes.yaml>`_.
Since v0.13 all the diagnostics are supposed to work with the AQUA convention, so that any other experiment following the AQUA convention will be compatible with the diagnostics.

.. _metadata-fix:

Metadata Correction
^^^^^^^^^^^^^^^^^^^^

The **vars** block in the ``fixer_name`` represent a list of variables that need
metadata correction: this covers units, names, grib codes, and any other metadata.
In addition, also new variables can be computed from pre-existing variables.

Merge of convention and fixer
=============================

If a convention is specified in the fixer file, the final fix dictionary will be merged with the convention file.
The priority is given to the ``fixer_name`` and it is done variable by variable.
This means that if a variable is present in both the convention and the fixer, the fixer will override
the subfields that are found in both. 

Let's consider an example where my variable ``tdswrf`` is already present in the convention file,
but I need to specify that it should be decumulated.
In the fixer we just need to add a detail to the convention, and this can be done without having
to specify all the details of the variable already present in the convention as shown in the following example:

.. code-block:: yaml

    convention_name:
        eccodes-2.39.0:
            convention: eccodes
            version: 2.39.0
            vars:
                tdswrf: # Top downward short-wave radiation flux
                    source:
                        - 260676
                        - 235053 # avg_tdswrf
                        - tdswrf
                        - avg_tdswrf
                        - mtdwswrf
                        - tisr
                    grib: 260676
    
    fixer_name:
        documentation-fix:
            convention: eccodes
            vars:
                tdswrf:
                    decumulate: true

The final block for the variable ``tdswrf`` will be:

.. code-block:: yaml

    vars:
        tdswrf:
            source:
                - 260676
                - 235053
                - tdswrf
                - avg_tdswrf
                - mtdwswrf
                - tisr
            grib: 260676
            decumulate: true

Variables block structure
=========================

The section :ref:`fix-structure` provides an exhaustive list of cases. 
In order to create a fix for a specific variable, two approaches are possibile:

- **source**: it will modify an existent variable changing its name (e.g from ``tp`` to ``tprate``).
  This will eventually be merged with the convention file as described in the previous section.
- **derived**: it will create a new variable, which can also be obtained with basic operations between
  multiple variables (e.g. getting ``mtntrf2`` from ``ttr`` + ``tsr``). 

.. warning ::
    Please note that only basic operation (sum, division, subtraction and multiplication) are allowed in the ``derived`` block

Then, extra keys can be then specified for **each** variable to allow for further fine tuning:

- **grib**: if set to a number, the fixer will associate the variable with the GRIB ParamID.
  This is possible since AQUA v0.13 and it is the preferred way to retrieve metadata.
  If set ``True``, the fixer will look for GRIB ShortName associated with the new variable and 
  will retrieve the associated metadata.
- **src_units**: override the source unit in case of specific issues (e.g. units which cannot be processed by MetPy).
- **units**: override the target unit.
- **decumulate**: if set to ``True``, activate the decumulation of the variable.
- **attributes**: with this key, it is possible to define a dictionary of attributes to be modified. 
  Please refer to the example in section :ref:`fix-structure`
  to see the possible implementation. 
- **mindate**: used to set to NaN all data before a specified date. 
  This is useful when dealing with data that are not available for the whole period of interest or which are partially wrong.

.. warning ::
    Recursive fixes (i.e. fixes of fixes) cannot be implemented. For example, it is not possibile to derive a variable from a derived variable

.. _coord-fix:

Data Model and Coordinates/Dimensions Correction
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The fixer can adopt a common **coordinate data model**. The default is the **aqua** data model,
which is a simplified version of the CF data model and is stored in the ``config/data_model/aqua.yaml`` folder.
If this data model is not appropriate for a specific source,
it is possible to specify a different one in the catalog source, but it has to be defined accordingly in the config folder.

.. warning ::
  Data model is being refactored which means that behaviour may change in the future.

If the data model coordinate treatment is not enough to fix the coordinates or dimensions,
it is possible to specify a custom fix in the catalog in the **coords** or **dims** blocks
as shown in section :ref:`fix-structure`.
For example, if the longitude coordinate is called ``longitude`` instead of ``lon``,
it is possible to specify a fix like:

.. code-block:: yaml

    coords: 
        lon:
            source: longitude

This will rename the coordinate to ``lon``.

.. note::
    When possible, prefer a **data model** treatment of coordinates and use the **coords**
    block as second option.

Similarly, if units are ill-defined in the dataset, it is possible to override them with the same fixer structure. 
Of course, this feature is valid only for **coords**:

.. code-block:: yaml

    coords: 
        level:
            tgt_units: m

.. warning::
    Please keep in mind that coordinate units is simply an override of the attribute. It won't make any assumption on the source units and will not convert it accordingly.

Develop your own fix
^^^^^^^^^^^^^^^^^^^^

If you need to develop your own, fixes can be added to the ``config/fixes`` folder.
This can be done using the ``fixer_name`` definitions, to be then provided as a metadata in the catalog source entry.
This represents fixes that have a common nickname which can be used in multiple sources when defining the catalog.
There is the possibility of specifing a **parent** fix so that a fix can be re-used with minor corrections,
merging small changes to a larger ``fixer_name``.

If the ``fixer_name`` is following a convention, it is possible to merge the fixer with the convention file
as described in :ref:`fix-structure`.

.. warning::  
    Please note that a source-based definition exists as the older AQUA implementation and will be deprecated
    in favour of the new approach described above.
    We strongly suggest to use the new approach for new fixes.

.. note::
    Since v0.13, the default fixer is deprecated. The fixer will first look for a convention file and then for a fixer file.
    If no ``fixer_name`` is provided and ``fix`` is set to ``True``, the code will look for a
    ``fixer_name`` called ``<MODEL_NAME>-default``. At the current stage of implementation, this is still merged with the convention file.

Please note that the ``default.yaml`` is reserved to define a few of useful tools:

- the default ``data_model`` (See :ref:`coord-fix`).
- the list of units that should be added to the default MetPy unit list. 
- A series of nicknames (``shortname``) for units to be replaced in the fixes yaml file.
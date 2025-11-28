Introduction
============

Overview of AQUA
----------------

AQUA (Climate DT Applications for QUality Assessment)
is a model evaluation framework designed for accessing and running diagnostics on high-resolution data produced by climate models.
The package provides a flexible and efficient framework to process and analyze large volumes of climate data. 
With its modular design, AQUA offers seamless integration of core functions and a wide range of diagnostic 
tools that can be run in parallel.

The repository for the AQUA core functionalities (data access and preprocessing) is the `AQUA-core <https://github.com/DestinE-Climate-DT/AQUA>`_ repository and the documentation refers to this repository for core functionalities.
The diagnostics are implemented in the `AQUA-diagnostics <https://github.com/DestinE-Climate-DT/AQUA-diagnostics>`_ repository and their documentation is available at the `AQUA-diagnostics documentation site <https://aqua-diagnostics.readthedocs.io/en/latest/>`_.

The AQUA-diagnostics repository contains the full set of diagnostic tools developed for the `Destination Earth Adaptation Climate Digital Twin (ClimateDT) <https://destine.ecmwf.int/news/aqua-application-quality-assessment-climate-dt-destination-earth/>`_.
It is designed to be used together with the AQUA core framework which provides data access and preprocessing functionalities.

Purpose
-------

The purpose of AQUA core is to allow an easy access and processing of high-resolution climate models outputs, 
making it easier for researchers and scientists to analyze and interpret climate data. 
AQUA core aims to provide a comprehensive toolkit for data preparation on climate model outputs.

Key Features
------------

- Efficient handling of large datasets from high-resolution climate models
- Support for various data formats, such as GRIB, NetCDF, Zarr, FDB, ARCO and Parquet access
- Robust and fast regridding functionality
- Averaging and aggregation tools for temporal and spatial analyses
- Metadata and coordinate fixes for data homogenization and comparison
- Modular design for easy expansion and integration of new functionalities
- Lazy data access and parallel processing for faster execution of processing pipelines, with limited memory usage

Contributing
------------

AQUA core is developed under the European Union Contract `DE_340_CSC - Destination Earth Programme
Climate Adaptation Digital Twin (Climate DT)`.
Contributions to the project are welcome and can be made through the GitHub repository.
Please refer to the Contribution Guidelines contained in the ``CONTRIBUTING.md`` file
in the repository for more information.

# Tropical Cyclones diagnostic for tracking and zoom in

Main authors: 
- Paolo Ghinassi (CNR-ISAC, p.ghinassi@isac.cnr.it)
- Paolo Davini (CNR-ISAC, p.davini@isac.cnr.it)

## Description

A diagnostic to identify tropical cyclones (TCs) centres (mean sea level pressure minima) and compute their trajectories based 
on the tempest-extremes python library (https://climate.ucdavis.edu/tempestextremes.php). In addition to detection and tracking
it features the possibility to save selected variables in a box in the vicinity of TCs centres along trajectories.


## Table of Contents

* [Installation Instructions](#installation-instructions)

  - [Installation on Levante](#installation-on-levante)

  - [Installation on Lumi](#installation-on-lumi)

* [Data requirements](#data-requirements)

* [Examples](#examples)

* [Contributing](#contributing)

## Installation Instructions

### Installation

The diagnostic is based on the AQUA framework, and requires the installation of the AQUA package. Follow the instructions in the AQUA documentation to install the framework. A pyproject.toml file is provided in this folder to install the diagnostic in the AQUA environment. It is not tought to be used as a standalone package.

## Data requirements  

Variables needed to perform TCs detection and tracking through tempest-extremes are:

Input variables
---------------

- `msl`     (Mean sea level pressure, GRIB paramid 151)
- `z`       (Geopotential height, GRIB paramid 129 at 300 and 500 hPa and at the surface (orography))
- `10u`     (2m zonal wind, GRIB pramid 165)
- `10v`     (2m meridional wind, GRIB pramid 166)

with 6 hourly resolution on a 1x1 degree regular grid. Examples of variables in which the zoom in is applied
such that they are stored in a netcdf file along the TCs tracks are:

- `msl`     (Mean sea level pressure, GRIB paramid 151)
- `10u`     (10m zonal wind, GRIB pramid 165)
- `10v`     (10m meridional wind, GRIB pramid 166)
- `pr`      (Total precipitation, GRIB pramid 228)
- `10fg`    (10m wind gust since the last postprocessing, GRIB pramid 49)

## Examples

The **notebook/** folder contains the following notebooks:

- **tropical_cyclones.ipynb**: 
  Notebook to explain and demonstrate the tropical cyclones diagnostic. It includes the main diagnostic feature which are the detection and tracking of tropical cyclones (usually performed on low resolution data) and the zoom in feature. The zoom in allows to save
  some selected variables in the vicinity of tropical cyclones centres along their trajectories using original resolution data.

## Contributing

The tempest-extremes library is available at: https://climate.ucdavis.edu/tempestextremes.php
Person responsible dor the development and mainteinance of the tropical cyclones diagnostic is Paolo Ghinassi (p.ghinass@isac.cnr.it)
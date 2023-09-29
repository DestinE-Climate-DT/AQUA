# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

Unreleased is the current development version.

- Speed up of the `retrieve_plain` method (#524)
- Update documention for adding new data and setting up the container (Increase documentation coverage #519)
- CLI wrapper for the state-of-the-art diagnostics analysis (#517)
- Refactor the regrid.yaml as grid-based instead of experiment-based (#291)
- aqua_common environment simplified and updated (#498)
- Update available variables in FDB catalogues on lumi (#514)
- Solve reversed latitudes bug for fixed data (#510)
- Switch to legacy eccodes tables based on intake source metadata (#493)
- Add GPM IMERG precipitation data to the catalogue on levante (#505)
- Fix ocean3d diagnostic colorbars not being symmetric when missing values are present (#504) 
- FDB NEMO test access to data (#488)
- Xarray dask access to FDB (#476)
- Issue a warning when multiple gribcodes are associated to the same shortname (Cases for multiple eccodes grib codes #483)
- Allowing fixer to overwrite or merge default configuration (Increasing flexibiity of the fixer allowing for merge, replace and default options #480)
- Add new tests (Increase testing #250)
- Global time series diagnostic setup for multiple variables CLI (#474)
- Option to avoid incomplete chunk when averagin with timmean (Introduce check for chunk completeness in timmean() #466)
- Simplification of Fixer() workflow, more methods and less redundancy (Functionize fixer #478)
- Remove the `aqua` environment file, only `aqua_common` is left (#482)

## [v0.3]

Main changes are:
1. Fixer moved at `Reader()` level
2. Area selection available in `fldmean()` method
3. FDB/GSV access for IFS-NEMO development simulations
4. Configuration file `config-aqua.yaml` replaces `config.yaml`

Complete list:
- Templates in configuration yaml files (#469)
- Bug fixes for FDB access options (#463, #462)
- Add observational catalogs on Lumi (Update Lumi catalog #454)
- Automatic finding of cdo (#456)
- Area is fixed if data are fixed (Fixer applied to grid areas #442)
- Tests missing failure fix (Fix #436 CI workflow passes even if some tests fail #452)
- FDB/GSV access to IFS control and historical simulations (#434, #458)
- Climatology support restored in the Reader (Fix for climatology #445)
- Improvement function to inspect the catalogue (Inspect_catalogue improvement #446)
- Minor improvements of the gribber (Fix gribber fdb #427)
- Allow the LRA generator to work with generators and so with FDB (LRA from fdb on mafalda #430)
- Fixes only on selected variables (Fixer updates #428)
- Complete revision of the FDB/GSV access, allowing to access also recent experiments using variable step (#343)
- Teleconnections diagnostic adapted to new code improvements (Teleconnections Dev branch update #424, #465)
- Add support for area selection with fldmean (Fldmean box selection #409)
- Environment simplified, dependencies are now mostly on the pyproject file (A simpler environment.yml #286)
- Intake esm functionality added back (Fix intake-esm #287)
- Intake esm tests (Test also intake-esm #335)
- Yaml dependencies removed (Logger and yaml issues in util.py #334)
- Log history working for iterators as well (Logger and yaml issues in util.py #334)
- Util refactor (Utility refactor #405)
- Fixer at reader level (Fixes at Reader level #244)
- Uniform timmean (Uniform time after timmean and add option for time_bnds #419)
- FDB tests added (Add FDB 5.11, a local FDB with some test data #280, #432)
- Refactor of unit conversion and non-metpy cases (Flexible unit fix from YAML file #416)
- Refactor of the config file definition (Refactor of the configuration search #417)

## [v0.2.1]

- Add development control-1950 and historical-1990 experiments to the LRA (LRA for control-1950 and historical-1990 on Levante from v0.2 #455)

## [v0.2]

- Improve the LRA generator and worklow CLI (Streaming for the LRA #289)
- AQUA new common environment installation tool for LUMI added (#413)
- Added a bash script "load_aqua_lumi.sh" to load aqua environment in LUMI with containers (Adding an AQUA singularity container for LUMI #418)

## [v0.2-beta]

This is the `AQUA` version part of the Deliverable D340.7.1.2. 

- SSH diagnostic improvements (Linting SSH diagnostics #377, SSH diag: PDF file name changed #388)
- Timmean fix to uniform time axis (Fix for timmean() to uniform output time axis #381)
- New tests trigger routine (Tests trigger with label #385)
- Fix for tco1279 and FESOM (fix for masked tco1279 #390, psu fix for salinity #383)
- ECmean improvements (various improvement for ecmean #392)
- Seaice diagnostic improvements (Deliverable340.7.1.2 fix seaice #389, Linting Seaice diagnostics #376)
- Teleconnections diagnostic graphics module enhanced and various improvements (Teleconnections corrections for D340.7.1.2 #379, Fix import in teleconnections notebooks #395, Teleconnections fix docs #408)
- Tropical cyclones linting of the diagnostic (Linting tropical cyclones diagnostics #380, Improved plotting functions for tropical cyclones #391)
- Ocean diagnostics restructured in a single folder, sharing common functions and other improvements (Linting+Fixes Ocean diagnostics #374, Adding units for MLD plot in ocean3d package #406)
- Documentation fixes (Documentation fixes after review #403)
- Atmglobalmean and radiation diagnostic improvements (Atmglobalmean fix #371)
- MSWEP fixer bugfix (Change MSWEP datamodel #397, fixing of mswep #401)

## [v0.2-alpha]

This is the `AQUA` version that will be part of the Deliverable D340.7.1.2, sent to internal review. This is mostly done by the inclusion of twelve diagnostics within the AQUA framework

- Added teleconnections diagnostic (#308, #309, #318, #333, #352)
- Added tropical cyclones diagnostic (#310, #345)
- Added performance indices diagnostic based on ECmean tool (#57, #327) 
- Added sea ice diagnostic (#353, #368)
- Added global timeseries diagnostic (#358, #359)
- Added radiation analysis diagnostic (#301, #360)
- Added global mean bias diagnostic (#285, #371)
- Added SSH variability diagnostic (#367, #369)
- Added tropical rainfall diagnostic (#314)
- Added Ocean circulation diagnostic (#295)
- Added global ocean diagnosc (#164)
- Added global mean timeseries (#268)
- Multiple fixes in the Reader (#316, #324, #334)
- Avoid time duplicated in the Reader (#357)
- Enabling autodoc for diagnostics (#330)
- Data access improvement on Levante, including new datasets (#332, #355, #321)
- Added a common environment file (#363)
- Support for Lumi installation (#315)
- Added the `changelog` file

### Changed

- Dummy diagnostic is now in the `dummy` folder (previously was `dummy-diagnostic`)
- Tests and code is now working with python>=3.9 (previously python 3.11 was excluded)

## [v0.1-beta]

This is the `AQUA` version that will be part of the Deliverable D340.7.1.1.
This is mostly built on the `AQUA` `Reader` class which support for climate model data interpolation, spatial and temporal aggregation and conversion for a common GRIB-like data format.


- Low resolution archive documentation
- Fixed a bug in the `Gribber` class that was not reading the correct yaml catalogue file

## v0.1-alpha

This is the AQUA pre-release to be sent to internal reviewers. 
Documentations is completed and notebooks are working.

[unreleased]: https://github.com/oloapinivad/AQUA/compare/v0.3...HEAD
[v0.3]: https://github.com/oloapinivad/AQUA/compare/v0.2.1...v0.3
[v0.2.1]: https://github.com/oloapinivad/AQUA/compare/v0.2...v0.2.1
[v0.2]: https://github.com/oloapinivad/AQUA/compare/v0.2-beta...v0.2
[v0.2-beta]: https://github.com/oloapinivad/AQUA/compare/v0.2-alpha...v0.2-beta
[v0.2-alpha]: https://github.com/oloapinivad/AQUA/compare/v0.1-beta...v0.2-alpha
[v0.1-beta]: https://github.com/oloapinivad/AQUA/compare/v0.1-alpha...v0.1-beta

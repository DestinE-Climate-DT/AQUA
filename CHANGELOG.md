# Changelog

All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)

## [Unreleased]

Unreleased in the current development version:
- The notebook with a demo of aqua components for Lumi (#854)
- Radiation: adaptation to new data governance and many improvements (#727)
- Seaice: Sea ice extent has now seasonal cycle (#797)
- Fixing the paths in `cli/lumi-install/lumi_install.sh` (#856).
- Refactor of the documentation (#842)
- The drop warning in `aqua/gsv/intake_gsv.py` (#844)

## [v0.6.3]

Complete list:
- Setting last date for NaN fix for IFS-NEMO/IFS-FESOM to 1999-10-01 and cleaner merge of parent fixes (#819)
- Hotfix to set `intake==0.7.0` as default (#841)
- Timeseries: can add annual std and now default uncertainty is 2 std (#830)
- `retrieve_plain()` method now set off startdate and enddate (#829)
- Complete restructure of fixer to make use of `fixer_name`: set a default for each model and a `False` to disable it (#746)
- Added `center_time` option in the `timmean()` method to save the time coordinate in the middle of the time interval and create a Timmean module and related TimmeanMixin class (#811)
- Fixer to rename coordinates available (#822)
- Fixing new pandas timedelta definition: replacing H with h in all FDB catalog (#786)
- Change environment name from `aqua_common` to `aqua`(#805)
- Adding a run test label to trigger CI (#826)
- Tropical_rainfall: improve organization and maintainability, introducing nested classes (#814)
- Revisiting CERES fixes (#833)
- Timeseries: add bands for observation in Gregory plots (#837)

## [v0.6.2]

Complete list:
- Global time series plot annual and monthly timeseries together, improved Gregory plot (#809)
- Teleconnection can now take a time range as input and ylim in the index plot function (#799)
- LRA to use `auto` final time and `exclude_incomplete` (#791)
- Hotfix for v0.12.0 of the GSV_interface related to valid_time (#788)
- Global time series adapted to new data governance (#785)
- AtmoGlobalMean diagnostic improvements and adaptation to new data governance (#745 #789 #807 #812)
- Sea-ice diagnostic adapted to new data governance (#790)
- Implement a fix setting to NaN the data of the first step in each month (for IFS historical-1990) (#776)

## [v0.6.1]

Complete list:
- Teleconnection improvement to accept different variable names for ENSO (avg_tos instead of sst) (#778)
- ERA5 fixes compatible with new data governance (#772)
- Update the LRA generator (removing aggregation and improving) filecheck and fix entries for historical-1990-dev-lowres (#772)
- Updates of ECmean to work with production experiments (#773, #780)
- Automatic data start and end dates for FDB sources (#762)

## [v0.6]

Main changes are:
1. Inclusion in the catalog of the historical-1990 production simulations from IFS-NEMO and IFS-FESOM.
2. New fixes that targets the DestinE updated Data Governance

- IFS-FESOM historical-1990-dev-lowres with new data governance added to the catalogue (#770)
- AtmoGlobalMean diagnostic improvements (#722)
- Teleconnections diagnostic improvements (#722)
- Read only one level for retrieving 3D array metadata, select single level for retrieve (#713)
- IFS-FESOM historical-1990-dev-lowres with new data governance added to the catalogue
- Fix mismatch between var argument and variables specified in catalogue for FDB (#761)
- Compact catalogues using yaml override syntax (#752)
- Fix loading source grid file before smmregrid weight generation (#756)

## [v0.5.2-beta]

Complete list:
-  A new fdb container is used to generate the correct AQUA container

## [v0.5.2-alpha]

Main changes are:
1. Coupled models IFS-NEMO and IFS-FESOM are now supported
2. Accessor to use functions and reader methods as if they were methods of xarray objects, see [notebook](https://github.com/DestinE-Climate-DT/AQUA/blob/main/notebooks/reader/accessor.ipynb)
3. Preliminary provenance information is now available in the history attribute of the output files
4. AQUA analysis wrapper is parallelized
5. A levelist can be provided in FDB sources, this will greatly speed up the data retrieve

Complete list:
- Fix reading only one sample variable and avoid _bnds variables (#743)
- Allow correct masked regridding after level selection. Add level selection also for not-FDB sources (#741)
- Read only one level for retrieving 3D array metadata, select specific levels for FDB retrieve (#713)
- Defining catalog entry for coupled models IFS-NEMO and IFS-FESOM (#720)
- Change fixer_name to fixer_name (#703)
- Reorganization of logging calls (#700)
- Accessor to use functions and reader methods as if they were methods of xarray objects (#716)
- Suggestions are printed if a model/exp/source is not found while inspecting the catalogue (#721)
- Improvements in the single map plot function (#717)
- Minor metadata fixes (logger newline and keep "GRIB_" in attrs) (#715)
- LRA fix now correctly aggregating monthly data to yearly when a full year is available (#696)
- History update and refinement creating preliminary provenance information (plus AQUA emoji!) (#676)
- OPA lra compatible with no regrid.yaml (#692)
- Introducing fixer definitions not model/exp/source dependents to be specified at the metadata level (#681)
- AQUA analysis wrapper is parallelized and output folder is restructured (#684, #725)

## [v0.5.1]

Main changes are:
1. A new `Reader` method `info()` is available to print the catalogue information
2. Grids are now stored online and a tool to deploy them on the `cli` folder is available

Complete list:
- Fix attributes of DataArrays read from FDB (#686)
- Reader.info() method to print the catalogue information (#683)
- Simpler reader init() by reorganizing the calls to areas and regrid weights configuration and loading (#682)
- Optional autosearch for vert_coord (#682)
- plot_single_map adapted to different coordinate names and bugfixes (#680)
- Sea ice volume datasets for the Northern Hemisphere (PIOMAS) and the Southern Hemisphere (GIOMAS) (#598)
- Possibility of defining the regrid method from the grid definition (#678)
- Grids stored online and tool to deploy them on cli folder (#675)
- Global time series diagnostic improvements (#637)
- Teleconnections diagnostic improvements (#672)

## [v0.5]

Main changes are:
1. Refactor of the Reader() interface with less options at the init() level
2. Grids are now defined with the source metadata and not in a machine-dependent file
3. CLI wrapper is available to run all diagnostics in a single call
4. Refactoring of the streaming emulator with equal treatment for FDB or file sources

Complete list:
- Controlling the loglevel of the GSV interface (#665)
- Fix wrong fdb source (#657)
- Adding sample files and tests for NEMO 2D and 3D grids (#652)
- tprate not derived from tp for GSV sources (#653)
- Simplify reader init and retrieve providing less argument in initialization (#620)
- var='paramid' can be used to select variables in the retrieve method (#648)
- configdir is not searched based on util file position in the repo (#636)
- Cleaner mask treatment (Revision of mask structure in the reader #617)
- Fldmean fix if only one dimension is present for area selection (#640)
- Adding higher frequency ERA5 data on Levante and Lumi (#628)
- regrid.yaml files are removed, grid infos are now in the catalogue metadata (#520, #622, #643)
- Load all available variables in FDB xarray/dask access (#619)
- Lint standard and enforced in CI (#616)
- Reader init split with methods (#523)
- Single map plot utility to be used by all diagnostics (#594)
- Script for automatic generation of Fdb catalog entries (IFS only) (#572)
- Fix loading of singularity mounting /projappl (#612)
- CLI wrapper parser (#599)
- Refactoring of streaming emulator (#593)
- Radiation CLI and diagnostic refinement (#537)
- Ocean3D CLI and diagnostic refinement (#578)
- AtmGlobalMean CLI and diagnostic refinement (#587)
- Tropical cyclones CLI refinements and TC module (#568, #645)
- Removing OPA, OPAgenerator and related tests from the AQUA (Remove OPA from AQUA #586)
- Renaming the experiments according to the DE340 AQUA syntax (Including dev-control-1990 in the source and rename the experiment according to DE340 scheme #556, #614, #618)
- Teleconnections diagnostic improvements (#571, #574, #576, #581, #592, #623)

## [v0.4]

Main changes are:
1. Update to all the diagnostics CLI
2. Refactor of the regridder so that `regrid.yaml`` is grid-based and not experiment-based
3. Xarray access to FDB sources
4. Refactor of the fixer so that merge/replace/default options are available
5. Remove of the `aqua` environment in favour of the `aqua_common` one. 

Complete list:
- Introduced color scheme for aqua logging (#567)
- CLI for sea diagnostic (#549)
- Add CLI for SSH diagnostic and some bug fixes (#540)
- Fix SSH diagnostic to be compatible with lates AQUA version (#538) 
- Helper function to identify vertical coordinates in a dataset (#552)
- Orography for tempest extremes TCs detection and update TCs CLI (Orography threshold included and CLI update #404)
- Improvement of performance indices CLI (Update of ECmean CLI #528)
- Fix to allow reading a list of multiple variables from FDB (#545)
- Further improvement of function to inspect the catalogue (#533)
- Custom exceptions for AQUA (#518)
- Speed up of the `retrieve_plain` method (#524)
- Update documention for adding new data and setting up the container (Increase documentation coverage #519)
- CLI wrapper for the state-of-the-art diagnostics analysis (#517, #527, #525, #530, #534, #536, #539, #548, #549, #559)
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

[unreleased]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.6.3...HEAD
[v0.6.3]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.6.2...v0.6.3
[v0.6.2]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.6.1...v0.6.2
[v0.6.1]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.6...v0.6.1
[v0.6]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.5.2-beta...v0.6
[v0.5.2-beta]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.5.2-alpha...v0.5.2-beta
[v0.5.2-alpha]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.5.1...v0.5.2-alpha
[v0.5.1]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.5...v0.5.1
[v0.5]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.4...v0.5
[v0.4]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.3...v0.4
[v0.3]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.2.1...v0.3
[v0.2.1]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.2...v0.2.1
[v0.2]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.2-beta...v0.2
[v0.2-beta]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.2-alpha...v0.2-beta
[v0.2-alpha]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.1-beta...v0.2-alpha
[v0.1-beta]: https://github.com/DestinE-Climate-DT/AQUA/compare/v0.1-alpha...v0.1-beta

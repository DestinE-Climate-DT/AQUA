"""Fixer mixin for the Reader class"""

import os
import re
import json
import warnings
import types
from datetime import timedelta
import xarray as xr
import numpy as np
import cf2cdm
from metpy.units import units

from aqua.util import eval_formula, get_eccodes_attr, find_lat_dir, check_direction
from aqua.logger import log_history

# Set the warning filter to always display DeprecationWarning
warnings.simplefilter('always', DeprecationWarning)


class FixerMixin():
    """Fixer mixin for the Reader class"""

    def find_fixes(self):
        """
        Get the fixes for the model/exp/source hierarchy.
        First looks for family fixes as base_fixes.
        If not found, looks for default fixes for the model.
        Then source_fixes are loaded and merged with base_fixes.

        Args:
            The fixer class

        Return:
            The fixer dictionary
        """

        # look for family fixes and set them as default
        base_fixes = self._load_fixer_name()

        # check the presence of model-specific fix
        fix_model = self.fixes_dictionary["models"].get(self.model, None)

        # browse for source-specific fixes
        source_fixes = self._load_source_fixes(fix_model)

        # if only fixes family/default is available, return them
        if source_fixes is None:
            if base_fixes is None:
                self.logger.warning("No fixes available for model %s, experiment %s, source %s",
                                    self.model, self.exp, self.source)
                return None
            else:
                self.logger.debug('Final fixes are: %s', base_fixes)
                return base_fixes

        # Join source specific fixes together with the found fixer_name
        if base_fixes is not None and source_fixes is not None:
            final_fixes = self._combine_fixes(base_fixes, source_fixes)
        elif base_fixes is None:
            final_fixes = source_fixes
        else:  # source_fixes is None
            final_fixes = base_fixes

        self.logger.debug('Final fixes are: %s', final_fixes)
        return final_fixes

    def _combine_fixes(self, default_fixes, model_fixes):
        """
        Combine fixes from the default or the source/model specific or the family fixes

        Args:
            default_fixes: Model default or family fixes
            model_fixes: Source specific fixes with ad hoc rules

        Returns:
            Final fix configuration
        """

        if model_fixes is None:
            # TODO; to be removed when restructuring the fixes
            # if default_fixes is None:
            #     self.logger.warning("No default fixes found! No fixes available for model %s, experiment %s, source %s",
            #                         self.model, self.exp, self.source)
            #     return None

            self.logger.info("Default model %s fixes found! Using it for experiment %s, source %s",
                             self.model, self.exp, self.source)
            return default_fixes
        else:
            # get method for replacement: replace is the default
            method = model_fixes.get('method', 'replace')
            self.logger.info("For source %s, method for fixes is: %s", self.source, method)

            # if nothing specified or replace method, use the fixes
            if method == 'replace':
                self.logger.debug("Replacing fixes with source-specific fixes")
                final_fixes = model_fixes

            # if merge method is specified, replace/add to default fixes
            elif method == 'merge':
                self.logger.debug("Merging fixes with source-specific fixes")
                final_fixes = self._merge_fixes(default_fixes, model_fixes)

            # if method is default, roll back to default
            elif method == 'default':
                self.logger.debug("Rolling back to default fixes")
                final_fixes = default_fixes

            return final_fixes

    def _load_fixer_name(self):
        """
        Load the fixer_name reading from the metadata of the catalog.
        If the fixer_name has a parent, load it and merge it giving priority to the child.
        """
        
        # if fixer name is found, get it
        if self.fixer_name is not None:
            self.logger.info('Fix names in metadata is %s', self.fixer_name)
            fixes = self.fixes_dictionary["fixer_name"].get(self.fixer_name)
            if fixes is None:
                self.logger.error("The requested fixer_name %s does not exist in fixes files", self.fixer_name)
                return None
            else:
                self.logger.info("Fix names %s found in fixes files", self.fixer_name)

                if 'parent' in fixes:
                    parent_fixes = self.fixes_dictionary["fixer_name"].get(fixes['parent'])
                    if parent_fixes is not None:
                        self.logger.info("Parent fix %s found! Mergin with fixer_name fixes %s!", fixes['parent'], self.fixer_name)
                        fixes = self._merge_fixes(parent_fixes, fixes)
                    else:
                        self.logger.error("Parent fix %s defined but not available in the fixes file.", fixes['parent'])

        # check the default in alternative
        else:   
            default_fixer_name = self.model + '-default'
            self.logger.info('No specific fix found, will call the default fix %s', default_fixer_name)
            fixes = self.fixes_dictionary["fixer_name"].get(default_fixer_name)
            if fixes is None:
                self.logger.warning("The requested default fixer name %s does not exist in fixes files", default_fixer_name)
                return None
            else:
                self.logger.info("Fix names %s found in fixes files", default_fixer_name)

        # # if found, proceed as expected
        # if fixes is not None:
        #     if self.fixer_name is not None:
        #         self.logger.info("Fix names %s found in fixes files", self.fixer_name)
        #     if 'parent' in fixes:
        #         parent_fixes = self.fixes_dictionary["fixer_name"].get(fixes['parent'])
        #         if parent_fixes is not None:
        #             self.logger.info("Parent fix %s found! Mergin with fixer_name fixes %s!", fixes['parent'], self.fixer_name)
        #             fixes = self._merge_fixes(parent_fixes, fixes)
        #         else:
        #             self.logger.error("Parent fix %s defined but not available in the fixes file.", fixes['parent'])
        # else:
        #     return None

        return fixes

    def _merge_fixes(self, base, specific):
        """
        Small function to merge fixes. Base fixes will be used as a default
        and specific fixes will replace where necessary. Dictionaries will be merged
        for variables, with priority for the specific ones.

        Args:
            base (dict): Base fixes
            specific (dict): Specific fixes

        Return:
            dict with merged fixes
        """
        final = base
        for item in specific.keys():
            if item == 'vars':
                final[item] = {**base[item], **specific[item]}
            else:
                final[item] = specific[item]

        return final

    def _load_source_fixes(self, fix_model):
        """Browse for source/model specific fixes, return None if not found"""

        if fix_model is None:
            self.logger.debug("No source-specific fixes available for model %s, using default fixes",
                              self.model)
            return None

        # look for exp fix, if not found, set default fixes
        fix_exp = fix_model.get(self.exp, None)
        if fix_exp is None:
            self.logger.debug("No source-specific fixes available for model %s, experiment %s",
                              self.model, self.exp)
            return None

        fixes = fix_exp.get(self.source, None)
        if fixes is None:
            self.logger.debug("No source-specific fixes available for model %s, experiment %s, source %s: checking for model default...",  # noqa: E501
                              self.model, self.exp, self.source)
            fixes = fix_exp.get('default', None)
            if fixes is None:
                self.logger.debug("Nothing found! I will use with model default or family fixes...")
            else:
                self.logger.debug("Using experiment-specific default for model %s, experiment %s", self.model, self.exp)
        else:
            self.logger.debug("Source-specific fixes found for model %s, experiment %s, source %s",
                              self.model, self.exp, self.source)

        return fixes

    def _load_default_fixes(self, fix_model):
        """
        Brief function to load the deafult fixes of single model.
        It looks at both the model and the experiment level.
        If default fixes are not found, return None

        Args:
            fix_model: The model dictionary of fixes

        Returns:
            dictionary of the default fixes
        """

        default_fix_exp = fix_model.get('default', None)
        if default_fix_exp is None:
            self.logger.info("Default fixes not found for %s", self.model)
            default_fixes = None
        else:
            if 'default' in default_fix_exp:
                default_fixes = default_fix_exp.get('default', None)
                if default_fixes is not None:
                    self.logger.debug("Default based fixes found for %s-%s", self.model, self.exp)

            else:
                self.logger.debug("Default based fixes found for %s", self.model)
                default_fixes = default_fix_exp

        return default_fixes

    def fixer(self, data, var, **kwargs):
        """Call the fixer function returning container or iterator"""
        if isinstance(data, types.GeneratorType):
            return self._fixergen(data, var, **kwargs)
        else:
            return self._fixer(data, var, **kwargs)

    def _fixergen(self, data, var, **kwargs):
        """Iterator version of the fixer"""
        for ds in data:
            yield self._fixer(ds, var, keep_memory=True, **kwargs)

    def _fixer(self, data, destvar, apply_unit_fix=True, keep_memory=False):
        """
        Perform fixes (var name, units, coord name adjustments) of the input dataset.

        Arguments:
            data (xr.Dataset):      the input dataset
            destvar (list of str):  the name of the desired variables to be fixed, if None all available variables are fixed
            apply_unit_fix (bool):  if to perform immediately unit conversions (which requite a product or an addition).
                                    The fixer sets anyway an offset or a multiplicative factor in the data attributes.
                                    These can be applied also later with the method `apply_unit_fix`. (true)
            keep_memory (bool):     if to keep memory of the previous fields (used for decumulation of iterators)

        Returns:
            A xarray.Dataset containing the fixed data and target units, factors and offsets in variable attributes.
        """

        # OLD NAMING SCHEME
        # unit: name of 'units' attribute
        # src_units: name of fixer source units
        # newunits: name of fixer target units

        # NEW NAMING SCHEME
        # tgt_units: target unit
        # fixer_src_units: name of fixer source units
        # fixer_tgt_units: name of fixer target units

        # Add extra units (might be moved somewhere else, function is at the bottom of this file)

        # Fix GRIB attribute names. This removes "GRIB_" from the beginning
        # for var in data.data_vars:
        #    data[var].attrs = {key.split("GRIB_")[-1]: value for key, value in data[var].attrs.items()}

        units_extra_definition()

        # if there are no fixes defined, return
        if self.fixes is None:
            return data

        # Default input datamodel
        src_datamodel = self.fixes_dictionary["defaults"].get("src_datamodel", None)
        self.logger.debug("Default input datamodel: %s", src_datamodel)

        self.deltat = self.fixes.get("deltat", 1.0)
        jump = self.fixes.get("jump", None)  # if to correct for a monthly accumulation jump
        # Special feature to fix corrupted data in first step of each month

        nanfirst_startdate = self.fixes.get("nanfirst_startdate", None)
        nanfirst_enddate = self.fixes.get("nanfirst_enddate", None)

        fixd = {}  # variables dictionary for name change: only for source
        varlist = {}  # variable dictionary for name change
        vars_to_fix = self.fixes.get("vars", None)  # variables with available fixes

        # check which variables need to be fixed among the requested ones
        vars_to_fix = self._check_which_variables_to_fix(vars_to_fix, destvar)

        if vars_to_fix:
            for var in vars_to_fix:

                # dictionary of fixes of the single var
                varfix = vars_to_fix[var]

                # get grib attributes if requested and fix name
                grib = varfix.get("grib", None)
                if grib:
                    attributes, shortname = self._get_variables_grib_attributes(var)
                else:
                    attributes = {}
                    shortname = var

                # Get extra attributes from fixer, leave empty dict otherwise
                attributes.update(varfix.get("attributes", {}))

                # define the list of name changes
                varlist[var] = shortname

                # 1. source case
                source = varfix.get("source", None)
                # if we are using a gribcode as a source, convert it to shortname to access it
                if str(source).isdigit():
                    self.logger.info(f'The source {source} is a grib code, need to convert it')
                    source = get_eccodes_attr(f'var{source}', loglevel=self.loglevel)['shortName']
                # This is a renamed variable. This will be done at the end.
                if source:
                    if source not in data.variables:
                        continue
                    if source != shortname:
                        fixd.update({f"{source}": f"{shortname}"})

                    log_history(data[source], f"Variable renamed {shortname} by fixer")

                # 2. derived case: let's compute the formula it and create the new variable
                formula = varfix.get("derived", None)
                if formula:
                    try:
                        source = shortname
                        data[source] = eval_formula(formula, data)
                        attributes.update({"derived": formula})
                        self.logger.debug("Derived %s from %s", var, formula)
                        log_history(data[source], f"Variable {var}, derived with {formula} by fixer")
                    except KeyError:
                        # The variable could not be computed, let's skip it
                        if destvar is not None:
                            # issue an error if you are asking that specific variable!
                            self.logger.error('Requested derived variable %s cannot be computed, is it available?', shortname)
                        else:
                            self.logger.info('%s is defined in the fixes but cannot be computed, is it available?',
                                                shortname)
                        continue

                # safe check debugging
                self.logger.debug('Name of fixer var: %s', var)
                self.logger.debug('Name of data source var: %s', source)
                self.logger.debug('Name of target var: %s', shortname)

                # fix source units
                data = self._override_src_units(data, varfix, var, source)

                # update attributes to the data but the units
                tgt_units = None
                if attributes:
                    for att, value in attributes.items():
                        # Already adjust all attributes but not yet units
                        if att == "units":
                            tgt_units = value
                        else:
                            data[source].attrs[att] = value

                tgt_units = self._override_tgt_units(tgt_units, varfix, var)

                if "units" not in data[source].attrs:  # Houston we have had a problem, no units!
                    self.logger.error('Variable %s has no units!', source)

                # adjust units
                if tgt_units:

                    if tgt_units.count('{'):  # WHAT IS THIS ABOUT?
                        tgt_units = self.fixes_dictionary["defaults"]["units"]["shortname"][tgt_units.replace('{',
                                                                                                              '').replace('}',
                                                                                                                          '')]
                    self.logger.info("%s: converting units %s --> %s", var, data[source].units, tgt_units)
                    log_history(data[source], f"Converting units of {var}: from {data[source].units} to {tgt_units}")
                    factor, offset = self.convert_units(data[source].units, tgt_units, var)
                    # self.logger.info('Factor: %s, offset: %s', factor, offset)

                    if (factor != 1.0) or (offset != 0):
                        data[source].attrs.update({"tgt_units": tgt_units})
                        data[source].attrs.update({"factor": factor})
                        data[source].attrs.update({"offset": offset})
                        self.logger.debug("Fixing %s to %s. Unit fix: factor=%f, offset=%f",
                                          source, var, factor, offset)
                        log_history(data[source], f"Fixing {source} to {var}. Unit fix: factor={factor}, offset={offset}")

        # Only now rename everything
        data = data.rename(fixd)

        # decumulate if necessary and fix first of month if necessary
        if vars_to_fix:
            data = self._wrapper_decumulate(data, vars_to_fix, varlist, keep_memory, jump)
            if nanfirst_enddate:  # This is a temporary fix for IFS data, run ony if an end date is specified
                data = self._wrapper_nanfirst(data, vars_to_fix, varlist,
                                              startdate=nanfirst_startdate,
                                              enddate=nanfirst_enddate)

        if apply_unit_fix:
            for var in data.data_vars:
                self.apply_unit_fix(data[var])

        # remove variables following the fixes request
        data = self._delete_variables(data)

        # Fix coordinates according to a given data model
        src_datamodel = self.fixes.get("data_model", src_datamodel)
        if src_datamodel:
            data = self.change_coord_datamodel(data, src_datamodel, self.dst_datamodel)
            self.logger.info(f"coordinates adjusted to {src_datamodel} by AQUA fixer")
            data = log_history(data, f"Coordinates adjusted to {src_datamodel} by fixer")

        # Extra coordinate handling
        data = self._fix_coord(data)

        return data

    def _delete_variables(self, data):
        """
        Remove variables which are set to be deleted in the fixer
        """

        # remove variables which should be deleted
        dellist = [x for x in self.fixes.get("delete", []) if x in data.variables]
        if dellist:
            data = data.drop_vars(dellist)

        return data

    def _wrapper_decumulate(self, data, variables, varlist, keep_memory, jump):
        """
        Wrapper function for decumulation, which takes into account the requirement of
        keeping into memory the last step for streaming/fdb purposes

        Args:
            Data: Xarray Dataset
            variables: The fixes of the variables
            varlist: the variable dictionary with the old and new names
            keep_memory: if to keep memory of the previous fields (used for decumulation of iterators)
            jump: the jump for decumulation

        Returns:
            Dataset with decumulated fixes
        """

        fkeep = False
        if keep_memory:
            data1 = data.isel(time=-1)  # save last timestep for possible future use
        for var in variables:
            # Decumulate if required
            if variables[var].get("decumulate", None):
                varname = varlist[var]
                if varname in data.variables:
                    self.logger.debug("Starting decumulation for variable %s", varname)
                    keep_first = variables[var].get("keep_first", True)
                    if keep_memory:  # Special case for iterators
                        fkeep = True  # We have decumulated at least once and we need to keep data
                        if not self.previous_data:
                            previous_data = xr.zeros_like(data[varname].isel(time=0))
                        else:
                            previous_data = self.previous_data[varname]
                        data[varname] = self.simple_decumulate(data[varname],
                                                               jump=jump,
                                                               keep_first=keep_first,
                                                               keep_memory=previous_data)
                    else:
                        data[varname] = self.simple_decumulate(data[varname],
                                                               jump=jump,
                                                               keep_first=keep_first)
                    log_history(data[varname], f"Variable {varname} decumulated by fixer")
        if fkeep:
            self.previous_data = data1  # keep the last timestep for further decumulations

        return data

    def _wrapper_nanfirst(self, data, variables, varlist, startdate=None, enddate=None):
        """
        Wrapper function for settting to nan first step of each month.
        This allows to fix an issue with IFS data where the first step of each month is corrupted.

        Args:
            Data: Xarray Dataset
            variables: The fixes of the variables
            varlist: the variable dictionary with the old and new names
            startdate: date before which to fix the first timestep of each month (could be False)
            enddate: date after which to fix the first timestep of each month (could be False)

        Returns:
            Dataset with data on first step of each month set to NaN
        """

        for var in variables:
            fix = variables[var].get("nanfirst", False)
            if fix:
                varname = varlist[var]
                if varname in data.variables:
                    self.logger.debug("Setting first step of months before %s and after %s to NaN for variable %s",
                                      enddate, startdate, varname)
                    log_history(data[varname], f"Fixer set first step of months before {enddate} and after {startdate} to NaN")
                    data[varname] = self.nanfirst(data[varname], startdate=startdate, enddate=enddate)

        return data

    def nanfirst(self, data, startdate=False, enddate=False):
        """
        Set to NaN the first step of each month before and/or after a given date for an xarray

        Args:
            data: Xarray DataArray
            startdate: date before which to fix the first timestep of each month (defaults to False)
            enddate: date after which to fix the first timestep of each month (defaults to False)

        Returns:
            DataArray in with data on first step of each month is set to NaN
        """

        first = data.time.groupby(data['time.year']*100+data['time.month']).first()
        if enddate:
            first = first.where(first < np.datetime64(str(enddate)), drop=True)
        if startdate:
            first = first.where(first > np.datetime64(str(startdate)), drop=True)
        mask = data.time.isin(first)
        data = data.where(~mask, np.nan)

        return data

    def _override_tgt_units(self, tgt_units, varfix, var):
        """
        Override destination units for the single variable
        """

        # Override destination units
        fixer_tgt_units = varfix.get("units", None)
        if fixer_tgt_units:
            self.logger.debug('Variable %s: Overriding target units "%s" with "%s"',
                              var, tgt_units, fixer_tgt_units)
            return fixer_tgt_units
        else:
            return tgt_units

    def _override_src_units(self, data, varfix, var, source):
        """
        Override source units for the single variable
        """

        # Override source units
        fixer_src_units = varfix.get("src_units", None)
        if fixer_src_units:
            if "units" in data[source].attrs:
                self.logger.debug('Variable %s: Overriding source units "%s" with "%s"',
                                  var, data[source].units, fixer_src_units)
                data[source].attrs.update({"units": fixer_src_units})
            else:
                self.logger.debug('Variable %s: Setting missing source units to "%s"',
                                  var, fixer_src_units)
                data[source].attrs["units"] = fixer_src_units

        return data

    def _get_variables_grib_attributes(self, var):
        """
        Get grib attributes for a specific variable

        Args:
            vardict: Variables dictionary with fixes
            var: variable name

        Returns:
            Dictionary for attributes following GRIB convention and string with updated variable name
        """
        self.logger.debug("Grib variable %s, looking for attributes", var)
        try:
            attributes = get_eccodes_attr(var, loglevel=self.loglevel)
            shortname = attributes.get("shortName", None)
            self.logger.debug("Grib variable %s, shortname is %s", var, shortname)

            if var not in ['~', shortname]:
                self.logger.debug("For grib variable %s find eccodes shortname %s, replacing it", var, shortname)
                var = shortname

            self.logger.debug("Grib attributes for %s: %s", var, attributes)
        except TypeError:
            self.logger.warning("Cannot get eccodes attributes for %s", var)
            self.logger.warning("Information may be missing in the output file")
            self.logger.warning("Please check your version of eccodes")

        return attributes, var

    def _check_which_variables_to_fix(self, var2fix, destvar):
        """
        Check on which variables fixes should be applied

        Args:
            var2fix: Variables for which fixes are available
            destvar: Variables on which we want to apply fixes

        Returns:
            List of variables on which we want to apply fixes for which fixes are available
        """

        if destvar and var2fix:  # If we have a list of variables to be fixed and fixes are available
            newkeys = list(set(var2fix.keys()) & set(destvar))
            if newkeys:
                var2fix = {key: value for key, value in var2fix.items() if key in newkeys}
                self.logger.info("Variables to be fixed: %s", var2fix)
            else:
                var2fix = None
                self.logger.info("No variables to be fixed")

        return var2fix

    def _fix_area(self, area: xr.DataArray):
        """
        Apply fixes to the area file

        Arguments:
            area (xr.DataArray):  area file to be fixed

        Returns:
            The fixed area file (xr.DataArray)
        """
        if self.fixes is None:  # No fixes available
            return area
        else:
            self.logger.debug("Applying fixes to area file")
            # This operation is a duplicate, rationalization with fixer method is needed
            src_datamodel = self.fixes_dictionary["defaults"].get("src_datamodel", None)
            src_datamodel = self.fixes.get("data_model", src_datamodel)

            if src_datamodel:
                area = self.change_coord_datamodel(area, src_datamodel, self.dst_datamodel)

            return area

    def _fix_coord(self, data: xr.Dataset):
        """
        Other than the data_model we can apply other fixes to the coordinates
        reading them from the fixes file, in the coords section.

        Arguments:
            data (xr.Dataset):  input dataset to process

        Returns:
            The processed input dataset
        """
        if self.fixes is None:
            return data

        coords_fix = self.fixes.get("coords", None)

        if coords_fix:
            coords = list(coords_fix.keys())
            self.logger.debug("Coordinates to be checked: %s", coords)

            for coord in coords:
                src_coord = coords_fix[coord].get("source", None)

                if src_coord and src_coord in data.coords:
                    data = data.rename({src_coord: coord})
                    self.logger.debug("Coordinate %s renamed to %s", src_coord, coord)
                    log_history(data[coord], f"Coordinate {src_coord} renamed to {coord} by fixer")
                else:
                    self.logger.warning("Coordinate %s not found", coord)

        return data

    def get_fixer_varname(self, var):
        """
        Load the fixes and check if the variable requested is there

        Args:
            var (str or list):  The variable to be checked

        Return:
            A list of variables to be loaded
        """

        if self.fixes is None:
            self.logger.debug("No fixes available")
            return var

        variables = self.fixes.get("vars", None)
        if variables:
            self.logger.debug("Variables in the fixes: %s", variables)
        else:
            self.logger.warning("No variables in the fixes for source %s",
                                self.source)
            self.logger.warning("Returning the original variable")
            return var

        # double check we have a list
        if isinstance(var, str):
            var = [var]

        # if a source/derived is available in the fixes, replace it
        loadvar = []
        for vvv in var:
            if vvv in variables.keys():

                # get the source ones
                if 'source' in variables[vvv]:
                    loadvar.append(variables[vvv]['source'])

                # get the ones from the equation of the derived ones
                if 'derived' in variables[vvv]:
                    # filter operations
                    required = [s for s in re.split(r'[-+*/]', variables[vvv]['derived']) if s]
                    # filter constants
                    required_strings = [x for x in required if not x.replace('.', '').isnumeric()]
                    if bool(set(required_strings) & set(variables.keys())):
                        self.logger.error("Recursive fixer definition: %s are variables defined in the fixer!",
                                          required_strings)
                        raise KeyError((
                                    "Recursive fixer definition are not supported when selecting variables or working with FDB sources."  # noqa: E501
                                    "Please change the fixes or call the retrieve() without the var arguments")
                                    )

                    loadvar = loadvar + required_strings
            else:
                loadvar.append(vvv)

        self.logger.debug("Variables to be loaded: %s", loadvar)
        return loadvar

    def simple_decumulate(self, data, jump=None, keep_first=True, keep_memory=None):
        """
        Remove cumulative effect on IFS fluxes.

        Args:
            data (xr.DataArray):     field to be processed
            jump (str):              used to fix periodic jumps (a very specific NextGEMS IFS issue)
                                     Examples: jump='month' (the NextGEMS case), jump='day')
            keep_first (bool):       if to keep the first value as it is (True) or place a 0 (False)
            keep_memory (DataArray): data from previous step

        Returns:
            A xarray.DataArray where the cumulation has been removed
        """

        # get the derivatives
        deltas = data.diff(dim='time')

        # add a first timestep empty to align the original and derived fields

        if keep_first:
            zeros = data.isel(time=0)
        else:
            zeros = xr.zeros_like(data.isel(time=0))
        if isinstance(keep_memory, xr.DataArray):
            data0 = data.isel(time=0)
            keep_memory = keep_memory.assign_coords(time=data0.time)  # We need them to have the same time
            zeros = data0 - keep_memory

        deltas = xr.concat([zeros, deltas], dim='time').transpose('time', ...)

        if jump:
            # universal mask based on the change of month (shifted by one timestep)
            dt = np.timedelta64(timedelta(seconds=self.deltat))
            data1 = data.assign_coords(time=data.time - dt)
            data2 = data.assign_coords(time=data1.time - dt)
            # Mask of dates where month changed in the previous timestep
            mask = data1[f'time.{jump}'].assign_coords(time=data.time) == data2[f'time.{jump}'].assign_coords(time=data.time)

            # kaboom: exploit where
            deltas = deltas.where(mask, data)

        # add an attribute that can be later used to infer about decumulation
        deltas.attrs['decumulated'] = 1

        return deltas

    def change_coord_datamodel(self, data, src_datamodel, dst_datamodel):
        """
        Wrapper around cfgrib.cf2cdm to perform coordinate conversions

        Arguments:
            data (xr.DataSet):      input dataset to process
            src_datamodel (str):    input datamodel (e.g. "cf")
            dst_datamodel (str):    output datamodel (e.g. "cds")

        Returns:
            The processed input dataset
        """
        fn = os.path.join(self.configdir, 'data_models', f'{src_datamodel}2{dst_datamodel}.json')
        self.logger.debug("Data model: %s", fn)
        with open(fn, 'r', encoding="utf8") as f:
            dm = json.load(f)

        if "IFSMagician" in data.attrs.get("history", ""):  # Special fix for gribscan levels
            if "level" in data.coords:
                if data.level.max() >= 1000:  # IS THERE A REASON FOR THIS CHECK?
                    data.level.attrs["units"] = "hPa"
                    data.level.attrs["standard_name"] = "air_pressure"
                    data.level.attrs["long_name"] = "pressure"

        if "GSV interface" in data.attrs.get("history", ""):  # Special fix for FDB retrieved data
            if "height" in data.coords:
                data.height.attrs["units"] = "hPa"
                data.height.attrs["standard_name"] = "air_pressure"
                data.height.attrs["long_name"] = "pressure"

        lat_coord, lat_dir = find_lat_dir(data)
        # this is needed since cf2cdm issues a (useless) UserWarning
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            data = cf2cdm.translate_coords(data, dm)
            # Hack needed because cfgrib.cf2cdm mixes up coordinates with dims
            if "forecast_reference_time" in data.dims:
                data = data.swap_dims({"forecast_reference_time": "time"})

        check_direction(data, lat_coord, lat_dir)  # set 'flipped' attribute if lat direction has changed
        return data

    def convert_units(self, src, dst, var="input var"):
        """
        Converts source to destination units using metpy.

        Arguments:
            src (str):  source units
            dst (str):  destination units
            var (str):  variable name (optional, used only for diagnostic output)

        Returns:
            factor, offset (float): a factor and an offset to convert the input data (None if not needed).
        """

        src = self.normalize_units(src)
        dst = self.normalize_units(dst)
        factor = units(src).to_base_units() / units(dst).to_base_units()

        if factor.units == units('dimensionless'):
            offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
        else:
            if factor.units == "meter ** 3 / kilogram":
                # Density of water was missing
                factor = factor * 1000 * units("kg m-3")
                self.logger.debug("%s: corrected multiplying by density of water 1000 kg m-3",
                                 var)
            elif factor.units == "meter ** 3 * second / kilogram":
                # Density of water and accumulation time were missing
                factor = factor * 1000 * units("kg m-3") / (self.deltat * units("s"))
                self.logger.debug("%s: corrected multiplying by density of water 1000 kg m-3",
                                 var)
                self.logger.info("%s: corrected dividing by accumulation time %s s",
                                 var, self.deltat)
            elif factor.units == "second":
                # Accumulation time was missing
                factor = factor / (self.deltat * units("s"))
                self.logger.debug("%s: corrected dividing by accumulation time %s s",
                                 var, self.deltat)
            elif factor.units == "kilogram / meter ** 3":
                # Density of water was missing
                factor = factor / (1000 * units("kg m-3"))
                self.logger.debug("%s: corrected dividing by density of water 1000 kg m-3", var)
            else:
                self.logger.debug("%s: incommensurate units converting %s to %s --> %s",
                                 var, src, dst, factor.units)
            offset = 0 * units(dst)

        if offset.magnitude != 0:
            factor = 1
            offset = offset.magnitude
        else:
            offset = 0
            factor = factor.magnitude
        return factor, offset

    def apply_unit_fix(self, data):
        """
        Applies unit fixes stored in variable attributes (target_units, factor and offset)

        Arguments:
            data (xr.DataArray):  input DataArray
        """
        tgt_units = data.attrs.get("tgt_units", None)
        org_units = data.attrs.get("units", None)
        self.logger.debug("org_units is %s, tgt_units is %s", org_units, tgt_units)

        # if units are not already updated and if a tgt_units exist
        if tgt_units and org_units != tgt_units:
            self.logger.debug("Applying unit fixes for %s ", data.name)

            # define an old units
            data.attrs.update({"src_units": org_units, "units_fixed": 1})
            data.attrs["units"] = self.normalize_units(tgt_units)
            factor = data.attrs.get("factor", 1)
            offset = data.attrs.get("offset", 0)
            if factor != 1:
                data *= factor
            if offset != 0:
                data += offset
            log_history(data, f"Units changed to {tgt_units} by fixer")
            data.attrs.pop('tgt_units', None)

    def normalize_units(self, src):
        """
        Get rid of crazy grib units based on the default.yaml fix file

        Arguments:
            src (str): input unit to be fixed
        """
        src = str(src)
        fix_units = self.fixes_dictionary['defaults']['units']['fix']
        for key in fix_units:
            if key == src:
                # return fixed
                self.logger.info('Replacing non-metpy unit %s with %s', key, fix_units[key])
                return src.replace(key, fix_units[key])

        # return not fixed
        return src


def units_extra_definition():
    """Add units to the pint registry"""

    # special units definition
    # needed to work with metpy 1.4.0 see
    # https://github.com/Unidata/MetPy/issues/2884
    units._on_redefinition = 'ignore'
    units.define('fraction = [] = Fraction = frac')
    units.define('psu = 1e-3 frac')
    units.define('PSU = 1e-3 frac')
    units.define('Sv = 1e+6 m^3/s')

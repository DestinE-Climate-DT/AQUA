"""Fixer mixin for the Reader class"""
import re
import numpy as np
from aqua.core.util import to_list, convert_units, get_eccodes_attr
from aqua.core.logger import log_history, log_configure
from .fixer_operator import FixerOperator
from .fixer_datamodel import FixerDataModel
from .fixer_configure import FixerConfigure
from .evaluate_formula import EvaluateFormula

DEFAULT_DELTAT = 1

class Fixer():
    """Fixer module
    
    Args:
        fixer_name (str): The fixer name defined in the fixes dictionary
        datamodel (str): The target datamodel name
        fixes_dictionary (dict): The dictionary of fixes
        convention (name): The convention name
        metadata (dict): The metadata dictionary
        loglevel (str): The log level
    
    """

    def __init__(self, fixer_name=None, fixes_dictionary=None,
                 convention=None, metadata=None, loglevel='WARNING'):

        self.fixes_dictionary = fixes_dictionary
        self.fixer_name = fixer_name
        self.convention = convention
        self.metadata = metadata
        self.logger = log_configure(log_level=loglevel, log_name='Fixer')
        self.loglevel = loglevel

        # loading all the configuration aspects of the fixer to be sent to the operator
        self.fixerconfigure = FixerConfigure(
            convention=self.convention, fixes_dictionary=self.fixes_dictionary, 
            fixer_name=self.fixer_name, loglevel=loglevel)
        self.fixes = self.fixerconfigure.find_fixes()
        self.deltat = self._define_deltat(default=DEFAULT_DELTAT)
        self.time_correction = False

        # this is the fixes operator, called internally by the fixer
        self.operator = FixerOperator(self.fixes, loglevel=loglevel)

        # this is the datamodel fixer, which applies supplementary fixes, called by the Reader
        self.fixerdatamodel = FixerDataModel(self.fixes, loglevel=loglevel)

    def fixer(self, data, destvar, apply_unit_fix=True):
        """
        Perform fixes (var name, units, coord name adjustments) of the input dataset.

        Arguments:
            data (xr.Dataset):      the input dataset
            destvar (list of str):  the name of the desired variables to be fixed, if None all available variables are fixed
            apply_unit_fix (bool):  if to perform immediately unit conversions (which requite a product or an addition).
                                    The fixer sets anyway an offset or a multiplicative factor in the data attributes.
                                    These can be applied also later with the method `apply_unit_fix`. (true)
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

        # if there are no fixes defined, return
        if self.fixes is None:
            return data

        # Default input datamodel
        #src_datamodel = self.fixes_dictionary["defaults"].get("src_datamodel", None)
        #self.logger.debug("Default input datamodel: %s", src_datamodel)

      
        # Special case for monthly deltat
        if self.deltat == "monthly":
            self.logger.info('%s deltat found, we will estimate a correction based on number of days per month', self.deltat)
            self.deltat = 3600*24
            self.time_correction = data.time.dt.days_in_month

        jump = self.fixes.get("jump", None)  # if to correct for a monthly accumulation jump

        # Special feature to fix corrupted data in first step of each month
        nanfirst_startdate = self.fixes.get("nanfirst_startdate", None)
        nanfirst_enddate = self.fixes.get("nanfirst_enddate", None)

        fixd = {}  # variables dictionary for name change: only for source, done as {source: var}
        varlist = {}  # variable dictionary for name change
        vars_to_fix = self.fixes.get("vars", None)  # variables with available fixes

        # check which variables need to be fixed among the requested ones
        vars_to_fix = self._check_which_variables_to_fix(vars_to_fix, destvar)

        if vars_to_fix:
            for var in vars_to_fix:

                # Dictionary of fixes of the single var
                varfix = vars_to_fix[var]

                # Get grib attributes if requested and fix name
                # This can be expanded to other formats in the future
                grib = varfix.get("grib", None)
                # We make sure also of the case were an user saw a grib: True
                # and decided to build a grib: False instead of just not using
                # the block
                if grib is not None and grib is not False:
                    # grib: True means that we're just going to use the default grib attributes
                    # associated with the variable name var
                    if isinstance(grib, bool):
                        attributes, shortname = self._get_variables_grib_attributes(var)
                    # grib: paramid is an option, this means that the variable name may not correspond
                    # to the shortname that it can be found within the attributes
                    elif isinstance(grib, int):
                        attributes, shortname = self._get_variables_grib_attributes(f"var{grib}")
                    else:
                        raise ValueError("grib should be either a boolean or an integer")
                else:
                    attributes = {}
                    shortname = var

                # Get extra attributes from fixer, leave empty dict otherwise
                attributes.update(varfix.get("attributes", {}))

                # Define the list of name changes
                varlist[var] = shortname

                # 1. source case. We want to be able to work with a list of sources to scan
                source = to_list(varfix.get("source", None))
                # We want to process a list of sources
                if source:
                    match = list(set(source) & set(data.variables))
                    if match:
                        # Having more than a match should be a problem for a dataset, we do not raise an error
                        # but we warn the user
                        if len(match) > 1:
                            self.logger.error("Multiple matches found for variable %s: %s, the first one will be taken",
                                              var, match)
                        # Even if we have only a match, we make sure that source is a string
                        source = match[0]

                        # If a gribcode is the source match, convert it to shortname to access it
                        if str(source).isdigit():
                            self.logger.info('The source %s is a grib code, need to convert it', source)
                            source = get_eccodes_attr(f'var{source}', loglevel=self.loglevel)['shortName']

                        # Here we update the fixd dictionary with the source and the variable
                        # The rename is done as {source: var} and at the end of the function
                        # This because the source name could be used in the derived formula
                        fixd.update({f"{source}": f"{var}"})
                        if source != var:
                            # We keep in the history the original variable name only if it is different from the target
                            log_history(data[source], f"Variable renamed {var} from {source} by fixer")
                    else:  # if there is no match
                        # We do not know in advance if the source is available, so we loop over all the available in the final
                        # merge of the fixes
                        self.logger.debug('While fixing variable %s, no match found with sources %s', var, source)
                        continue

                # 2. derived case: let's compute the formula it and create the new variable
                formula = varfix.get("derived", None)
                if formula:
                    # If the formula is the same as the variable name, we raise an error
                    # Asking for a derived variable that is also a source variable is not allowed
                    # since it may lead to changing how the fixer based on the source variable is applied
                    if formula == var:
                        self.logger.error('Derived variable %s cannot have the same name as the source variable, skipping it',
                                          var)
                        continue
                    try:
                        source = shortname
                        data[source] = EvaluateFormula(data=data, formula=formula, short_name=shortname,
                                                       loglevel=self.loglevel).evaluate()
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
                    if data[source].units != tgt_units:
                        log_history(data[source], f"Converting units of {var}: from {data[source].units} to {tgt_units}")
                    conversion_dictionary = convert_units(data[source].units, tgt_units, deltat=self.deltat,
                                                          var=var, loglevel=self.loglevel)

                    # if some unit conversion is defined, modify the attributes and history for later usage
                    if conversion_dictionary:
                        data[source].attrs.update({"tgt_units": tgt_units})
                        for key, value in conversion_dictionary.items():
                            data[source].attrs.update({key: value})
                            self.logger.debug("Fixing %s to %s. Unit fix: %s=%f", source, var, key, float(value))
                            log_history(data[source], f"Fixing {source} to {var}. Unit fix: {key}={value}")
                    elif conversion_dictionary == {} and data[source].units != tgt_units:
                        self.logger.info("No conversion needed for %s, but units are renamed from %s to %s",
                                         var, data[source].units, tgt_units)
                        data[source].attrs.update({"units": tgt_units})

                # Set to NaN before a certain date
                mindate = varfix.get("mindate", None)
                if mindate:
                    data[source] = data[source].where(data.time >= np.datetime64(str(mindate)), np.nan)
                    data[source].attrs.update({"mindate": mindate})
                    self.logger.debug("Steps before %s set to NaN for variable %s", str(mindate), var)

        # Only now rename everything
        for item, value in fixd.items():
            if not data[item].attrs.get("derived", None):
                data = data.rename({item: value})
            else:
                self.logger.info("Variable %s is derived, it will not be renamed to %s", item, value)

        # decumulate if necessary and fix first of month if necessary
        if vars_to_fix:
            data = self.operator.wrapper_decumulate(data, self.deltat, vars_to_fix, varlist, jump)
            if nanfirst_enddate:  # This is a temporary fix for IFS data, run ony if an end date is specified
                data = self.operator.wrapper_nanfirst(data, vars_to_fix, varlist,
                                              startdate=nanfirst_startdate,
                                              enddate=nanfirst_enddate)

        if apply_unit_fix:
            for var in data.data_vars:
                self.operator.apply_unit_fix(data[var], time_correction=self.time_correction)

        # apply time shift if necessary
        data = self.operator.timeshifter(data)

        # remove variables following the fixes request
        data = self.operator.delete_variables(data)
        
        return data

    def _define_deltat(self, default):
        """
        Define the deltat for the fixer. 
        The priority is given to the metadata, then to the fixes and finally to the default value.
        Return deltat in seconds.
        """

        # First case: get from metadata
        metadata_deltat = self.metadata.get('deltat')
        if metadata_deltat:
            self.logger.debug('deltat = %s read from metadata', metadata_deltat)
            return metadata_deltat

        # Second case if not available: get from fixes
        fix_deltat = self.fixes.get("deltat")
        if fix_deltat:
            self.logger.debug('deltat = %s read from fixes', fix_deltat)
            return fix_deltat
        
        # Third case: get from default
        self.logger.debug('deltat = %s defined as Fixer() default', default)
        return default

 
    
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
            attributes = get_eccodes_attr(var, loglevel=self.loglevel).copy()  # The copy is needed because the function in eccodes.py is cached
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


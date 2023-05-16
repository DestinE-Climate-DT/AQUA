"""Fixer mixin for the Reader class"""

import os
import json
import warnings
import xarray as xr
import cf2cdm
from metpy.units import units

from aqua.util import load_multi_yaml, _eval_formula, get_eccodes_attr
from aqua.util import log_history


class FixerMixin():
    """Fixer mixin for the Reader class"""

    def fixer(self, data, apply_unit_fix=False):
        """
        Perform fixes (var name, units, coord name adjustments) of the input dataset.

        Arguments:
            data (xr.Dataset):      the input dataset
            apply_unit_fix (bool):  if to perform immediately unit conversions (which requite a product or an addition).
                                    The fixer sets anyway an offset or a multiplicative factor in the data attributes.
                                    These can be applied also later with the method `apply_unit_fix`. (false)

        Returns:
            A xarray.Dataset containing the fixed data and target units, factors and offsets in variable attributes.
        """

        # add extra units (might be moved somewhere else)
        units_extra_definition()
        fixes = load_multi_yaml(self.fixer_folder)
        model = self.model
        exp = self.exp
        src = self.source

        # Default input datamodel
        src_datamodel = fixes["defaults"].get("src_datamodel", None)

        fixm = fixes["models"].get(model, None)
        if not fixm:
            self.logger.warning("No fixes defined for model %s", model)
            return data

        fixexp = fixm.get(exp, None)
        if not fixexp:
            fixexp = fixm.get('default', None)
            if not fixexp:
                self.logger.warning("No fixes defined for model %s, experiment %s", model, exp)
                return data

        fix = fixexp.get(src, None)
        if not fix:
            fix = fixexp.get('default', None)
            if not fix:
                self.logger.warning("No fixes defined for model %s, experiment %s, source %s", model, exp, src)
                return data

        self.deltat = fix.get("deltat", 1.0)
        jump = fix.get("jump", None)  # if to correct for a monthly accumulation jump

        fixd = {}
        varlist = {}
        variables = fix.get("vars", None)
        if variables:
            for var in variables:
                unit = None
                attributes = {}
                varname = var

                grib = variables[var].get("grib", None)
                # This is a grib variable, use eccodes to find attributes
                if grib:
                    # Get relevant eccodes attribues
                    attributes.update(get_eccodes_attr(var))
                    sn = attributes.get("shortName", None)
                    if (sn != '~') and (var != sn):
                        varname = sn
                        self.logger.info("Grib attributes for %s: %s", varname, attributes)

                varlist[var] = varname

                source = variables[var].get("source", None)
                # This is a renamed variable. This will be done at the end.
                if source:
                    if source not in data.variables:
                        continue
                    fixd.update({f"{source}": f"{varname}"})
                    log_history(data[source], "variable renamed by AQUA fixer")

                formula = variables[var].get("derived", None)
                # This is a derived variable, let's compute it and create the new variable
                if formula:
                    try:
                        data[varname] = _eval_formula(formula, data)
                        source = varname
                        attributes.update({"derived": formula})
                        self.logger.info("Derived %s from %s", var, formula)
                        log_history(data[source], "variable derived by AQUA fixer")
                    except KeyError:
                        # The variable could not be computed, let's skip it
                        continue

                # Get extra attributes if any
                attributes.update(variables[var].get("attributes", {}))

                # update attributes
                if attributes:
                    for att, value in attributes.items():
                        # Already adjust all attributes but not yet units
                        if att == "units":
                            unit = value
                        else:
                            data[source].attrs[att] = value

                # Override destination units
                newunits = variables[var].get("units", None)
                if newunits:
                    data[source].attrs.update({"units": newunits})
                    unit = newunits

                # Override source units
                src_units = variables[var].get("src_units", None)
                if src_units:
                    data[source].attrs.update({"units": src_units})

                # adjust units
                if unit:
                    if unit.count('{'):
                        unit = fixes["defaults"]["units"][unit.replace('{', '').replace('}', '')]
                    self.logger.info("%s: %s --> %s", var, data[source].units, unit)
                    factor, offset = self.convert_units(data[source].units, unit, var)
                    if (factor != 1.0) or (offset != 0):
                        data[source].attrs.update({"target_units": unit})
                        data[source].attrs.update({"factor": factor})
                        data[source].attrs.update({"offset": offset})
                        self.logger.info("Fixing %s to %s. Unit fix: factor=%f, offset=%f", source, var, factor, offset)

        # Only now rename everything
        data = data.rename(fixd)

        if variables:
            for var in variables:
                # Decumulate if required
                if variables[var].get("decumulate", None):
                    varname = varlist[var]
                    if varname in data.variables:
                        keep_first = variables[var].get("keep_first", True)
                        data[varname] = self.simple_decumulate(data[varname],
                                                               jump=jump,
                                                               keep_first=keep_first)
                        log_history(data[varname], "variable decumulated by AQUA fixer")

        if apply_unit_fix:
            for var in data.variables:
                self.apply_unit_fix(data[var])

        dellist = [x for x in fix.get("delete", []) if x in data.variables]
        if dellist:
            data = data.drop_vars(dellist)

        # Fix coordinates according to a given data model
        src_datamodel = fix.get("data_model", src_datamodel)
        if src_datamodel and src_datamodel is not False:
            data = self.change_coord_datamodel(data, src_datamodel, self.dst_datamodel)
            log_history(data, "coordinates adjusted by AQUA fixer")

        return data

    def simple_decumulate(self, data, jump=None, keep_first=True):
        """
        Remove cumulative effect on IFS fluxes.

        Args:
            data (xr.DataArray):     field to be processed
            jump (str):              used to fix periodic jumps (a very specific NextGEMS IFS issue)
                                     Examples: jump='month' (the NextGEMS case), jump='day')
            keep_first (bool):       if to keep the first value as it is (True) or place a 0 (False)

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

        deltas = xr.concat([zeros, deltas], dim='time').transpose('time', ...)

        if jump:
            # universal mask based on the change of month (shifted by one timestep)
            mask = ~(data[f'time.{jump}'] != data[f'time.{jump}'].shift(time=1))
            mask = mask.shift(time=1, fill_value=False)

            # kaboom: exploit where
            deltas = deltas.where(mask, data)

            # remove the first timestep (no sense in cumulated)
            # clean = clean.isel(time=slice(1, None))

        # add an attribute that can be later used to infer about decumulation
        deltas.attrs['decumulated'] = 1

        log_history(deltas, "decumulated by AQUA fixer")
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
        self.logger.info("Data model: %s", fn)
        with open(fn, 'r', encoding="utf8") as f:
            dm = json.load(f)

        if "IFSMagician" in data.attrs.get("history", ""):  # Special fix for gribscan levels
            if "level" in data.coords:
                if data.level.max() >= 1000:
                    data.level.attrs["units"] = "hPa"
                    data.level.attrs["standard_name"] = "air_pressure"
                    data.level.attrs["long_name"] = "pressure"

        # this is needed since cf2cdm issues a (useless) UserWarning
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=UserWarning)
            data = cf2cdm.translate_coords(data, dm)
            # Hack needed because cfgrib.cf2cdm mixes up coordinates with dims
            if "forecast_reference_time" in data.dims:
                data = data.swap_dims({"forecast_reference_time": "time"})
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

        src = normalize_units(src)
        dst = normalize_units(dst)
        factor = units(src).to_base_units() / units(dst).to_base_units()

        if factor.units == units('dimensionless'):
            offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
        else:
            if factor.units == "meter ** 3 / kilogram":
                # Density of water was missing
                factor = factor * 1000 * units("kg m-3")
                self.logger.info("%s: corrected multiplying by density of water 1000 kg m-3", var)
            elif factor.units == "meter ** 3 * second / kilogram":
                # Density of water and accumulation time were missing
                factor = factor * 1000 * units("kg m-3") / (self.deltat * units("s"))
                self.logger.info("%s: corrected multiplying by density of water 1000 kg m-3", var)
                self.logger.info("%s: corrected dividing by accumulation time %s s", var, self.deltat)
            elif factor.units == "second":
                # Accumulation time was missing
                factor = factor / (self.deltat * units("s"))
                self.logger.info("%s: corrected dividing by accumulation time %s s", var, self.deltat)
            elif factor.units == "kilogram / meter ** 3":
                # Density of water was missing
                factor = factor / (1000 * units("kg m-3"))
                self.logger.info("%s: corrected dividing by density of water 1000 kg m-3", var)
            else:
                self.logger.info("%s: incommensurate units converting %s to %s --> %s", var, src, dst, factor.units)
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
        target_units = data.attrs.get("target_units", None)
        if target_units:
            d = {"src_units": data.attrs["units"], "units_fixed": 1}
            data.attrs.update(d)
            data.attrs["units"] = normalize_units(target_units)
            factor = data.attrs.get("factor", 1)
            offset = data.attrs.get("offset", 0)
            if factor != 1:
                data *= factor
            if offset != 0:
                data += offset
            log_history(data, "units changed by AQUA fixer")


def normalize_units(src):
    """Get rid of crazy grib units"""
    if src == '1':
        return 'dimensionless'
    else:
        return str(src).replace("of", "").replace("water", "").replace("equivalent", "")
    
def units_extra_definition():
    """Add units to the pint registry"""

    # special units definition
    # needed to work with metpy 1.4.0 see
    # https://github.com/Unidata/MetPy/issues/2884
    units._on_redefinition = 'ignore'
    units.define('fraction = [] = frac')
    units.define('psu = 1e-3 frac')
    units.define('PSU = 1e-3 frac')
    units.define('Sv = 1e+6 m^3/s')

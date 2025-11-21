"""Strategies for fixing issues in the code."""

from datetime import timedelta
import numpy as np
import pandas as pd
import xarray as xr

from aqua.core.logger import log_history, log_configure
from aqua.core.util import to_list, normalize_units

class FixerOperator:
    """
    Base class for fix operators.
    Fix operators are method which apply specific fixes to the data.
    They all operate on xarray DataArray or Dataset objects.
    The FixOperator class is initialized with a dictionary of fixes and a log level.

    Args:
        fixes (dict): Dictionary containing the fixes to be applied.
        loglevel (int, optional): Log level for logging. Defaults to None.
    """

    def __init__(self, fixes, loglevel=None):

        self.fixes = fixes
        self.loglevel = loglevel
        self.logger = log_configure(log_level = loglevel, log_name="FixerOperator")

    def apply_unit_fix(self, data, time_correction=False):
        """
        Applies unit fixes stored in variable attributes (target_units, factor and offset)

        Arguments:
            data (xr.DataArray):  input DataArray
        """
        tgt_units = data.attrs.get("tgt_units", None)
        org_units = data.attrs.get("units", None)
        self.logger.debug("%s: org_units is %s, tgt_units is %s",
                        data.name, org_units, tgt_units)

        # if units are not already updated and if a tgt_units exist
        if tgt_units and org_units != tgt_units:
            self.logger.debug("Applying unit fixes for %s ", data.name)

            # define an old units
            data.attrs.update({"src_units": org_units, "units_fixed": 1})
            data.attrs["units"] = normalize_units(tgt_units)
            factor = data.attrs.get("factor", 1)
            offset = data.attrs.get("offset", 0)
            time_conversion_flag = data.attrs.get("time_conversion_flag", 0)
            if factor != 1:
                data *= factor
                # if a special dpm correction has been defined, apply it
                if time_conversion_flag and time_correction is not False:
                    data /=  time_correction
            if offset != 0:
                data += offset
            log_history(data, f"Units changed to {tgt_units} by fixer")
            data.attrs.pop('tgt_units', None)

    def delete_variables(self, data):
        """
        Remove variables which are set to be deleted in the fixer
        """

        # remove variables which should be deleted
        dellist = [x for x in to_list(self.fixes.get("delete", [])) if x in data.variables]
        if dellist:
            data = data.drop_vars(dellist)

        return data

    def timeshifter(self, data):
        """
        Apply a timeshift to the time coordinate of an xr.Dataset.

        Parameters:
        - data (xr.Dataset): The dataset containing a 'time' coordinate to be shifted.

        Returns:
        - xr.Dataset: The dataset with the 'time' coordinate shifted based on the specified timeshift
                      which is retrieved from the fixes dictionary.
        """
        timeshift = self.fixes.get('timeshift', None)

        if timeshift is None:
            return data

        if 'time' not in data:
            raise KeyError("'time' coordinate not found in the dataset.")

        field = data.copy()
        if isinstance(timeshift, int):
            self.logger.info('Shifting the time axis by %s timesteps.', timeshift)
            time_interval = timeshift * data.time.diff("time").isel(time=0).values
            field = field.assign_coords(time=data.time + time_interval)
        elif isinstance(timeshift, str):
            self.logger.info('Shifting time axis by %s following pandas timedelta.', timeshift)
            field['time'] = field['time'] + pd.Timedelta(timeshift)
        else:
            raise TypeError('timeshift should be either a integer (timesteps) or a pandas Timedelta!')

        return field

    def wrapper_decumulate(self, data, deltat, variables, varlist, jump):
        """
        Wrapper function for decumulation, which takes into account the requirement of
        keeping into memory the last step for streaming/fdb purposes

        Args:
            Data: Xarray Dataset
            variables: The fixes of the variables
            varlist: the variable dictionary with the old and new names
            jump: the jump for decumulation

        Returns:
            Dataset with decumulated fixes
        """

        for var in variables:
            # Decumulate if required
            if variables[var].get("decumulate", None):
                varname = varlist[var]
                if varname in data.variables:
                    self.logger.debug("Starting decumulation for variable %s", varname)
                    keep_first = variables[var].get("keep_first", True)
                    data[varname] = self.simple_decumulate(data[varname],
                                                           deltat=deltat,
                                                           jump=jump,
                                                           keep_first=keep_first)
                    log_history(data[varname], f"Variable {varname} decumulated by fixer")

        return data
    
    def simple_decumulate(self, data, deltat=3600, jump=None, keep_first=True):
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

        deltas = xr.concat([zeros, deltas], dim='time', coords='different', compat='equals').transpose('time', ...)

        if jump:
            # universal mask based on the change of month (shifted by one timestep)
            dt = np.timedelta64(timedelta(seconds=deltat))
            data1 = data.assign_coords(time=data.time - dt)
            data2 = data.assign_coords(time=data1.time - dt)
            # Mask of dates where month changed in the previous timestep
            mask = data1[f'time.{jump}'].assign_coords(time=data.time) == data2[f'time.{jump}'].assign_coords(time=data.time)

            # kaboom: exploit where
            deltas = deltas.where(mask, data)

        # add an attribute that can be later used to infer about decumulation
        deltas.attrs['decumulated'] = 1

        return deltas

    def wrapper_nanfirst(self, data, variables, varlist, startdate=None, enddate=None):
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
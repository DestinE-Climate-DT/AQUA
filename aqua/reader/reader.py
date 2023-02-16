import intake
import xarray as xr
import os
from metpy.units import units
import smmregrid as rg
from aqua.util import load_yaml
import sys

class Reader():
    """General reader for NextGEMS data (on Levante for now)"""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, freq=None,
                 regrid=None, method="ycon", zoom=None, configdir = 'config'):
        """
        The Reader constructor.
        It uses the cataolog `config/config.yaml` to identify the required data.
        
        Arguments:
            model (str):    the model ID
            exp (str):      experiment ID
            source (str):   source (ID)
            regrid (str):   perform regridding to grid `regrid`, as defined in `config/regrid.yaml` (None)
            method (str):   regridding method (ycon)
            zoom (int):     healpix zoom level
            configdir (str) Folder where the config/catalog files are (config)
        
        Returns:
            A `Reader` class object.
        """

        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        self.zoom = zoom
        self.freq = freq

        catalog_file = os.path.join(configdir, "catalog.yaml")
        self.cat = intake.open_catalog(catalog_file)

        cfg_regrid = load_yaml(os.path.join(configdir,"regrid.yaml"))

        if source:
            self.source = source
        else:
            self.source = list(self.cat[model][exp].keys())[0]  # take first source if none provided
        
        if regrid:
            self.weightsfile =os.path.join(
                cfg_regrid["weights"]["path"],
                cfg_regrid["weights"]["template"].format(model=model,
                                                    exp=exp, 
                                                    method=method, 
                                                    target=regrid))
            if os.path.exists(self.weightsfile):
                self.weights = xr.open_mfdataset(self.weightsfile)
            else:
                print("Weights file not found:", self.weightsfile)
                print("Attempting to generate it ...")

                weights = rg.cdo_generate_weights(source_grid=cfg_regrid["source_grids"][exp]["path"],
                                                      target_grid=cfg_regrid["target_grids"][regrid], 
                                                      method='ycon', 
                                                      gridpath=cfg_regrid["paths"]["grids"],
                                                      icongridpath=cfg_regrid["paths"]["icon"],
                                                      extra=cfg_regrid["source_grids"][exp].get("extra", None))
                weights.to_netcdf(self.weightsfile)
                self.weights = weights
                print("Success!")

            self.regridder = rg.Regridder(weights=self.weights)

               
    def retrieve(self, regrid=False, average=False, fix=True, apply_unit_fix=True):
        """
        Perform a data retrieve.
        
        Arguments:
            regrid (bool):          if to regrid the retrieved data (False)
            fix (bool):             if to perform a fix (var name, units, coord name adjustments) (True)
            apply_unit_fix (bool):  if to already adjust units by multiplying by a factor or adding
                                    an offset (this can also be done later with the `fix_units` method) (True)
        Returns:
            A xarray.Dataset containing the required data.
        """

        if self.zoom:
            data = self.cat[self.model][self.exp][self.source](zoom=self.zoom).to_dask()
        else:
            data = self.cat[self.model][self.exp][self.source].to_dask()

        # sequence which should be more efficient: averaging - regridding - fixing
        if self.freq and average:
            data = self.average(data)
        if self.targetgrid and regrid:
            data = self.regridder.regrid(data)  # XXX check attrs
        if fix:
            data = self.fixer(data, apply_unit_fix=apply_unit_fix)
        return data

    def regrid(self, data):
        """
        Perform regridding of the input dataset.
        
        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        out = self.regridder.regrid(data)
        #out.attrs = data.attrs #smmregrid exports now attributes
        return out
    
    def average(self, data):
        """
        Perform daily and monthly averaging

        Arguments:
            data (xr.Dataset):  the input xarray.Dataset
        Returns:
            A xarray.Dataset containing the regridded data.
        """

        if self.freq == 'mon':
            out = data.resample(time='M').mean()
        elif self.freq == 'day':
            out = data.resample(time='D').mean()
        else: 
            try: 
                out = data.resample(time=self.freq).mean()
            except: 
                sys.exit('Cant find a frequency to resample')
   
        return out
    
    def fixer(self, data, apply_unit_fix=False):
        """
        Perform fixes (var name, units, coord name adjustments) of the input dataset.
        
        Arguments:
            data (xr.Dataset):      the input dataset
            apply_unit_fix (bool):  if to perform immediately unit conversions (which requite a product or an addition). 
                                    The fixer sets anyway an offset or a multiplicative factor in the data attributes.
                                    These can be applied also later with the method `fix_units`. (false)

        Returns:
            A xarray.Dataset containing the fixed data and target units, factors and offsets in variable attributes.
        """

        fixes = load_yaml("config/fixes.yaml")
        model=self.model

        fix = fixes["models"].get(model, None)
        if not fix:
            print("No fixes defined for model ", model)
            return data

        self.deltat = fix.get("deltat", 1.0)
        fixd = {}
        allvars = data.variables
        for var in fix["vars"]:
            shortname = fix["vars"][var]["short_name"]
            if var in allvars: 
                fixd.update({f"{var}": f"{shortname}"})
                src_units = fix["vars"][var].get("src_units", None)
                if src_units:
                    data[var].attrs.update({"units": src_units})
                units = fix["vars"][var]["units"]
                if units.count('{'):
                    units = fixes["defaults"]["units"][units.replace('{','').replace('}','')]                
                data[var].attrs.update({"target_units": units})
                factor, offset = self.convert_units(data[var].units, units, var)
                data[var].attrs.update({"factor": factor})
                data[var].attrs.update({"offset": offset})
                if apply_unit_fix:
                    self.apply_unit_fix(data[var])

        allcoords = data.coords
        fixcoord = fix.get("coords", None)
        if fixcoord:
            for coord in fixcoord:
                    newcoord = fix["coords"][coord]["name"]
                    newcoord = fixes["defaults"]["coords"][newcoord.replace('{','').replace('}','')] 
                    if coord in allcoords: 
                        fixd.update({f"{coord}": f"{newcoord}"})

        return data.rename(fixd)

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
        factor = units(src).to_base_units() / units(dst).to_base_units()

        if factor.units == units('dimensionless'):
            offset = (0 * units(src)).to(units(dst)) - (0 * units(dst))
        else:
            if factor.units == "meter ** 3 / kilogram":
                # Density of water was missing
                factor = factor * 1000 * units("kg m-3")
                print(f"{var}: corrected multiplying by density of water 1000 kg m-3")
            if factor.units == "meter ** 3 * second / kilogram":
                # Density of water and accumulation time were missing
                factor = factor * 1000 * units("kg m-3") / (self.deltat * units("s"))
                print(f"{var}: corrected multiplying by density of water 1000 kg m-3")
                print(f"{var}: corrected dividing by accumulation time {self.deltat} s")
            else:
                print(f"{var}: incommensurate units converting {src} to {dst} --> {factor.units}")
            offset = 0 * units(dst)

        if offset.magnitude != 0:
            factor = None
            offset = offset.magnitude
        else:
            offset = None
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
            d = {"src_units": data.attrs["units"], "units_fixed": True}
            data.attrs.update(d)
            data.attrs["units"] = target_units
            factor = data.attrs.get("factor", None)
            offset = data.attrs.get("offset", None)
            if factor:
                data *= factor
            if offset:
                data += offset


        
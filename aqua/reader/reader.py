import intake
import xarray as xr
import os
import smmregrid as rg
from aqua.util import load_yaml

class Reader():
    """General reader for NextGEMS data (on Levante for now)"""

    def __init__(self, model="ICON", exp="tco2559-ng5", source=None, regrid=None, method="ycon", zoom=None):
        self.exp = exp
        self.model = model
        self.targetgrid = regrid
        self.zoom = zoom

        catalog_file = "config/catalog.yaml"
        self.cat = intake.open_catalog(catalog_file)

        cfg_regrid = load_yaml("config/regrid.yaml")

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
               
    def retrieve(self, regrid=False):
        if self.zoom:
            data = self.cat[self.model][self.exp][self.source](zoom=self.zoom).to_dask()
        else:
            data = self.cat[self.model][self.exp][self.source].to_dask()
        if self.targetgrid and regrid:
            data = self.regridder.regrid(data)
        return data

    def regrid(self, data):
        return self.regridder.regrid(data)

